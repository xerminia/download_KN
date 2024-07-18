package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/elastic/go-elasticsearch/v7"
)

func main() {
	startTime := time.Now()
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://ipev2-phd-access-es-01p.data.corp:9200/",
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

	// Чтение файла со списком sectionNumber
	file, err := os.Open("1.txt") // Имя файла
	if err != nil {
		log.Fatalf("Error opening file: %s", err)
	}
	defer file.Close()

	// Создание папки RR, если она не существует
	err = os.MkdirAll("RR", os.ModePerm)
	if err != nil {
		log.Fatalf("Error creating directory: %s", err)
	}

	var wg sync.WaitGroup
	scanner := bufio.NewScanner(file)
	resultChan := make(chan int, 10) // Переместили создание канала выше

	// go func() {
	// 	for size := range resultChan {
	// 		totalSize := size
	// 		elapsedTime := time.Since(startTime)
	// 		fmt.Printf("Вес составляет: %d bytes\nВремя работы программы: %s\n\n", totalSize, elapsedTime)
	// 	}
	// }()

	for scanner.Scan() {
		sectionNumber := scanner.Text()
		wg.Add(1)
		go func(sectionNumber string) {
			defer wg.Done()

			// Подстановка sectionNumber в запрос
			query := map[string]interface{}{
				"size": 10000, // Количество записей, которое мы хотим получить (допустим, нужно еще 1000)
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{
								"match": map[string]interface{}{
									"metadata.actual": "true",
								},
							},
							{
								"match": map[string]interface{}{
									"metadata.sectionNumber": sectionNumber, // Используем sectionNumber из файла
								},
							},
						},
					},
				},
			}

			scroll := "2m"
			scrollDuration, err := time.ParseDuration(scroll)
			if err != nil {
				log.Fatalf("Error parsing scroll duration: %s", err)
			}

			err = performScrollSearch(es, query, scrollDuration, resultChan, sectionNumber)
			if err != nil {
				log.Printf("Error during scroll search for sectionNumber %s: %s", sectionNumber, err)
			}
		}(sectionNumber)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %s", err)
	}

	wg.Wait()         // Ожидание завершения всех горутин
	close(resultChan) // Закрытие канала после завершения всех горутин

	elapsedTime := time.Since(startTime)
	fmt.Printf("Время работы программы: %s\n", elapsedTime)
}

func performScrollSearch(es *elasticsearch.Client, query map[string]interface{}, scroll time.Duration, resultChan chan<- int, sectionNumber string) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("registry_records_search_alias"),
		es.Search.WithBody(&buf),
		es.Search.WithScroll(scroll),
	)
	if err != nil {
		return fmt.Errorf("error initiating scroll search: %s", err)
	}
	defer res.Body.Close()

	for {
		if res.StatusCode != 200 {
			return fmt.Errorf("error in search response: %s", res.Status())
		}

		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return fmt.Errorf("error parsing response body: %s", err)
		}

		hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(hits) == 0 {
			break
		}

		for index, hit := range hits {
			doc := hit.(map[string]interface{})
			source := doc["_source"].(map[string]interface{})
			content := source["content"].(string)
			contentSize := len(content)
			resultChan <- contentSize

			// Замена символов ':' на '_'
			safeSectionNumber := strings.ReplaceAll(sectionNumber, ":", "_")

			// Генерация уникального имени файла
			filename := fmt.Sprintf("%s-%d.xml", safeSectionNumber, index)
			// Путь до файла в папке RR
			filePath := filepath.Join("RR", filename)

			// Создание файла в папке RR
			file, err := os.Create(filePath)
			if err != nil {
				return fmt.Errorf("error creating file: %s", err)
			}

			// Запись данных в файл
			if _, err := file.WriteString(content); err != nil {
				file.Close() // Закрываем файл перед возвратом ошибки
				return fmt.Errorf("error writing to file: %s", err)
			}

			file.Close() // Закрытие файла
		}

		runtime.GC()

		scrollID := r["_scroll_id"].(string)

		res, err = es.Scroll(
			es.Scroll.WithContext(context.Background()),
			es.Scroll.WithScrollID(scrollID),
			es.Scroll.WithScroll(scroll),
		)
		if err != nil {
			return fmt.Errorf("error continuing scroll search: %s", err)
		}
	}

	return nil
}

package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 18 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	// Виконання запиту до балансувальника
	for i := 0; i < 5; i++ {
		time.Sleep(6 * time.Second)
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
		t.Error(err)
		}
		t.Logf("response from [%s]", resp.Header.Get("lb-from"))
	}
}

func BenchmarkBalancer(b *testing.B) {
	// Імітуємо бенчмарк-тестування запиту до балансувальника
	for i := 0; i < b.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
	}
}

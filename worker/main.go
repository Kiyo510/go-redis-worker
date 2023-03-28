package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-redis/redis/v8"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func sendEmail(job *EmailJob) error {
	from := "example@example.com"
	password := "my-email-password"
	smtpHost := "smtp.example.com"
	smtpPort := "587"

	auth := smtp.PlainAuth("", from, password, smtpHost)
	to := []string{job.To}
	msg := []byte("To: " + job.To + "\r\n" +
		"Subject: " + job.Subject + "\r\n" +
		"\r\n" +
		job.Body + "\r\n")

	return smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, msg)
}

func processJob(ctx context.Context, rdb *redis.Client, jobData string) error {
	var job EmailJob
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return err
	}
	return sendEmail(&job)
}

func main() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "",
		DB:       0,
	})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ctx := context.Background()

	// Job queue key in Redis
	jobQueueKey := "job_queue"

	for {
		select {
		case <-stop:
			fmt.Println("Gracefully shutting down...")
			err := rdb.Close()
			if err != nil {
				fmt.Println(err)
				return
			}
			return
		default:
			job, err := rdb.BLPop(ctx, 0, jobQueueKey).Result()
			if err != nil {
				fmt.Println("Error polling job:", err)
				continue
			}

			err = processJob(ctx, rdb, job[1])
			if err != nil {
				fmt.Println("Error processing job:", err)
			}
		}
	}
}

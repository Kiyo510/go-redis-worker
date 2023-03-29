package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/smtp"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/go-redis/redis/v8"
)

type EmailJob struct {
	CommandName string `json:"commandName"`
	To          string `json:"to"`
	Subject     string `json:"subject"`
	HtmlContent string `json:"htmlContent"`
}

func sendEmail(to string, htmlContent string) error {
	from := "hoge-email@example.com"
	password := "hoge-email-password"
	smtpHost := "smtp.example.com"
	smtpPort := "587"

	auth := smtp.PlainAuth("", from, password, smtpHost)

	header := make(map[string]string)
	header["From"] = from
	header["To"] = to
	header["Subject"] = "Password Reset"
	header["MIME-Version"] = "1.0"
	header["Content-Type"] = `text/html; charset="utf-8"`
	header["Content-Transfer-Encoding"] = "base64"

	var message bytes.Buffer
	for k, v := range header {
		message.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	message.WriteString("\r\n")

	encodedContent := base64.StdEncoding.EncodeToString([]byte(htmlContent))
	lineMaxLength := 76
	for i := 0; i < len(encodedContent); i++ {
		if i > 0 && i%lineMaxLength == 0 {
			message.WriteString("\r\n")
		}
		message.WriteByte(encodedContent[i])
	}

	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, []string{to}, message.Bytes())
	if err != nil {
		fmt.Printf("Error while sending email: %v\n", err)
	} else {
		fmt.Println("Email sent successfully")
	}
	return nil
}

func processJob(job EmailJob) error {
	return sendEmail(job.To, job.HtmlContent)
}

func main() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	queueName := "default"
	jobQueueKey := fmt.Sprintf("queues:%s", queueName)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	for {
		select {
		case <-stop:
			fmt.Println("Gracefully shutting down... Waiting for running jobs to complete.")
			wg.Wait()
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

			var emailJob EmailJob
			err = json.Unmarshal([]byte(job[1]), &emailJob)
			if err != nil {
				fmt.Printf("Error while unmarshalling job: %v\n", err)
				continue
			}

			wg.Add(1) // ジョブが開始されたため、WaitGroupにカウントを追加
			go func() {
				defer wg.Done() // ジョブが完了したらWaitGroupのカウントをデクリメント

				err = processJob(emailJob)
				if err != nil {
					fmt.Println("Error processing job:", err)
				}
			}()
		}
	}
}

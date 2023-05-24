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
)

type EmailJob struct {
	CommandName string `json:"command_name"`
	To          string `json:"to"`
	Subject     string `json:"subject"`
	HtmlContent string `json:"html_content"`
}

func sendEmail(to, htmlContent, subject string) error {
	from := "info@example.net"
	password := "password"
	smtpHost := "localhost"
	smtpPort := "1025"
	auth := smtp.PlainAuth("", "maildev", password, smtpHost)

	// 件名をUTF-8に変換してエンコード
	subject = "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(subject)) + "?="
	header := make(map[string]string)
	header["From"] = from
	header["To"] = to
	header["Subject"] = subject
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
		return err
	}
	fmt.Println("Email sent successfully")

	return nil
}

func processJob(job EmailJob) error {
	return sendEmail(job.To, job.HtmlContent, job.Subject)
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	queueName := "default"
	jobQueueKey := fmt.Sprintf("%s", queueName)

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

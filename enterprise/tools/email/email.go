// Tool to help with email HTML development.
//
// Edit the code to experiment with email layouts/styles,
// then run this tool, piping the output to an HTML file.
// You can then open the HTML file in a Web browser.
//
// Once you've got a basic layout working locally, make sure to send a test
// email by appending the -send_to=<email> argument (a SendGrid API must be configured).

package main

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/sendgrid"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	e "github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications/email"
)

var (
	sendTo   = flag.String("send_to", "", "Send email to this address using sendgrid. By default, just dumps to stdout.")
	sendFrom = flag.String("send_from", "test@buildbuddy.io", "Send email from this address using sendgrid. Does nothing if -send_to is unset.")
)

const loremIpsum = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed
do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim
veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum
dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident,
sunt in culpa qui officia deserunt mollit anim id est laborum.`

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()
	ctx := context.Background()

	emailHTML := e.HTML(
		e.Header(),
		e.Title("Test email from BuildBuddy"),
		e.Card(
			e.Center(e.ImageAsset("circle-alert_orange_48px.png", 48, 48)),
			e.VGap(8),
			e.Center(e.Heading("Usage alert triggered")),
			e.P("Hello world!"),
			e.P(loremIpsum),
			e.CTA("https://app.buildbuddy.io/", "See builds"),
		),
		e.Footer(),
	)

	// Print the email inside a fake inbox layout
	fmt.Println(`<!DOCTYPE html>
<html style="margin:0;padding:0;">
<body style="margin:0;padding:160px;background:#eee;font-family:sans-serif;font-size:13px;">
<h1>email.example.com</h1>
<h2>Test email</h2>
<div style="background:white;border:1px solid red;">`)
	fmt.Println(emailHTML)
	fmt.Println(`</div></body></html>`)

	if *sendTo == "" {
		return nil
	}
	sg, err := sendgrid.NewClient()
	if err != nil {
		return err
	}
	return sg.SendEmail(ctx, "", "BuildBuddy (Test)", *sendFrom, []string{*sendTo}, "Test email", "text/html", emailHTML)
}

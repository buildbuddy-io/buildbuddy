// Email HTML builder.

package email

import (
	"fmt"
	"strings"
)

const assetsBucket = "https://storage.googleapis.com/buildbuddy-static/email_assets/"

// Returns the complete HTML contents for the email body.
// Typically should consist of calls to the following:
// - Header
// - Title (optional)
// - Body elements - ex: Card with Heading, P, and CTA
// - Footer
func HTML(contents ...string) string {
	return table("max-width:544px;margin:0 auto;", tr("", td("padding:16px;", Stack(contents...))))
}

// Returns the standard email header.
func Header() string {
	return ImageAsset("logo_dark.png", 256, 43)
}

// Email title, typically placed after Header.
// For org-level notifications
func Title(contents ...string) string {
	return `<h2 style="font-weight:normal;font-size:24px;">` + strings.Join(contents, "") + `</h2>`
}

// Footer text with top and bottom margin.
func Footer() string {
	return `<p style="line-height: 1;font-size:12px;color:#616161;text-align:center;margin:16px 0;">Iteration, Inc. ・ 2261 Market Street #4889, San Francisco, CA 94114</p>`
}

func Heading(content string) string {
	return `<h3 style="font-size:20px;margin-top:0;margin-bottom:16px;">` + content + `</h3>`
}

// Paragraph with bottom margin.
func P(contents ...string) string {
	return `<p style="line-height:1.25;margin-top:0;margin-bottom:16px;">` + strings.Join(contents, "") + `</p>`
}

// Bold text.
func B(content string) string {
	return `<b>` + content + `</b>`
}

// Outlined card with inner padding and no margin.
func Card(contents ...string) string {
	return table("max-width:544px;margin:0 auto;", tr("", td("display:block;border:1px solid #eee;border-radius:8px;padding:16px;", Stack(contents...))))
}

// Click-through action.
func CTA(href, content string) string {
	return table("", tr("", td("text-align:center;", A(href, "display:inline-block;padding:16px 32px;background:#212121;border-radius:8px;color:white;", content))))
}

// Inline text link.
func A(href, style, content string) string {
	return `<a href="` + href + `" style="color:#1E88E5;text-decoration:none;text-underline-offset:4px;` + style + `">` + content + `</a>`
}

// Vertically stacks the given contents with no gaps between them.
func Stack(items ...string) string {
	return table("", trtds(items...))
}

// Centers inline contents horizontally.
func Center(content string) string {
	return table("", tr("", td("text-align:center;", content)))
}

// Returns an image that has been uploaded to the GCS bucket.
// NOTE: SVG is unsupported
func ImageAsset(name string, width, height int) string {
	return fmt.Sprintf(`<img src="%s%s" width="%d" height="%d" />`, assetsBucket, name, width, height)
}

// Vertical spacer with the given height.
func VGap(height int) string {
	return fmt.Sprintf(`<table style="height:%dpx;"></table>`, height)
}

// Horizontal spacer with the given width.
func HGap(width int) string {
	return fmt.Sprintf(`<table style="width:%dpx;"></table>`, width)
}

// Table layout primitive. Tables are used for HTML emails because they have
// good support across email clients.
func table(style, content string) string {
	return `<table width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border-spacing:0;padding:0;margin:0;width:100%;` + style + `"><tbody>` + content + `</tbody></table>`
}

func tr(style, content string) string {
	return `<tr style="` + style + `">` + content + `</tr>`
}

func td(style, content string) string {
	return `<td style="` + style + `">` + content + `</td>`
}

func trs(contents ...string) string {
	out := make([]string, 0, len(contents))
	for _, c := range contents {
		out = append(out, tr("", c))
	}
	return strings.Join(out, "")
}

func tds(contents ...string) string {
	out := make([]string, 0, len(contents))
	for _, c := range contents {
		out = append(out, td("", c))
	}
	return strings.Join(out, "")
}

func trtds(contents ...string) string {
	out := make([]string, 0, len(contents))
	for _, c := range contents {
		out = append(out, tr("", td("", c)))
	}
	return strings.Join(out, "")
}

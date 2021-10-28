package webtester

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/bazelbuild/rules_webtesting/go/webtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"
)

// WebTester wraps selenium.WebDriver, failing the test instead of returning
// errors for all of its API methods.
type WebTester struct {
	t      *testing.T
	driver selenium.WebDriver
}

// New returns a WebTester scoped to the given test. It registers a cleanup
// function to record a screenshot if the test fails.
func New(t *testing.T) *WebTester {
	driver, err := webtest.NewWebDriverSession(selenium.Capabilities{
		chrome.CapabilitiesKey: chrome.Capabilities{
			Args: []string{
				// `--disable-dev-shm-usage` and `--no-sandbox` are a fix for
				// "DevToolsActivePort file doesn't exist" on docker
				// https://stackoverflow.com/questions/50642308
				"--disable-dev-shm-usage", "--no-sandbox",
				// Window size is specified as a flag as a workaround for
				// `SetWindowSize` returning an error in headless mode
				// https://github.com/yukinying/chrome-headless-browser-docker/issues/11
				"--window-size=1920,1000",
			},
		},
	})
	require.NoError(t, err, "failed to create webdriver session")
	wt := &WebTester{t, driver}
	t.Cleanup(func() {
		err := wt.screenshot("END_OF_TEST")
		// NOTE: `assert` here instead of `require` so that we still close down
		// the webdriver if the screenshot fails.
		assert.NoError(t, err, "failed to take end-of-test screenshot")

		err = driver.Quit()
		require.NoError(t, err)
	})
	return wt
}

// Get navigates to the given URL.
func (wt *WebTester) Get(url string) {
	err := wt.driver.Get(url)
	require.NoError(wt.t, err)
}

// Find returns the element matching the given CSS selector. Exactly one
// element must be matched, otherwise the test fails.
func (wt *WebTester) Find(cssSelector string) *Element {
	el, err := wt.driver.FindElement(selenium.ByCSSSelector, cssSelector)
	require.NoError(wt.t, err)
	return &Element{wt.t, el}
}

// Screenshot takes a screenshot and saves it in the test outputs directory. The
// given tag is used to disambiguate between other screenhots taken in the test.
func (wt *WebTester) Screenshot(tag string) {
	err := wt.screenshot(tag)
	require.NoError(wt.t, err, "failed to take screenshot")
}

func (wt *WebTester) screenshot(tag string) error {
	// TEST_UNDECLARED_OUTPUTS_DIR is usually defined by Bazel. If this test is
	// run outside of Bazel for whatever reason, this will just be an empty
	// string, which is interpreted by CreateTemp as "use the OS-default temp dir"
	testScreenshotDir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	screenshotFile, err := os.CreateTemp(testScreenshotDir, fmt.Sprintf("%s.%s.screenshot.png", wt.t.Name(), tag))
	if err != nil {
		return err
	}
	defer screenshotFile.Close()

	sc, err := wt.driver.Screenshot()
	if err != nil {
		return err
	}
	if _, err := io.Copy(screenshotFile, bytes.NewReader(sc)); err != nil {
		return err
	}
	wt.t.Logf("Screenshot saved to %s", screenshotFile.Name())
	return nil
}

// Element wraps selenium.WebElement, failing the test instead of returning
// errors for all of its API methods.
type Element struct {
	t          *testing.T
	webElement selenium.WebElement
}

// Click clicks on the element.
func (el *Element) Click() {
	err := el.webElement.Click()
	require.NoError(el.t, err)
}

// Text returns the text of the element.
func (el *Element) Text() string {
	txt, err := el.webElement.Text()
	require.NoError(el.t, err)
	return txt
}

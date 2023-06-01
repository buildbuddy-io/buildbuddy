package webtester

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_webtesting/go/webtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"
)

var (
	// To debug webdriver tests visually in your local environment:
	//
	// 1. Run the test with --config=webdriver-debug, which sets the
	//   --webdriver_debug flag below as well as some other necessary flags.
	//
	// 2. Optionally, set --webdriver_end_of_test_delay=1h to extend the
	//    end-of-test wait duration, and set --test_filter=NameOfTest to debug
	//    a specific failing test.

	debug               = flag.Bool("webdriver_debug", false, "Enable debug mode for webdriver tests.")
	endOfTestDelay      = flag.Duration("webdriver_end_of_test_delay", 3*time.Second, "How long to wait at the end of failed webdriver tests. Has no effect if --webdriver_debug is not set.")
	implicitWaitTimeout = flag.Duration("webdriver_implicit_wait_timeout", 1*time.Second, "Max time webtester should wait to find an element.")
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
	// Note, the chromeArgs and chromedriverArgs below are appended to the
	// default args defined here:
	// https://github.com/bazelbuild/rules_webtesting/blob/1460fa2b9a4307765cdf4989019a92ed6e65e51f/browsers/chromium-local.json
	chromeArgs := []string{
		// Window size is specified as a flag as a workaround for
		// `SetWindowSize` returning an error in headless mode
		// https://github.com/yukinying/chrome-headless-browser-docker/issues/11
		"--window-size=1920,1000",
	}
	chromedriverArgs := []string{}
	if *debug {
		// Remove the --headless arg applied to the "chromium-local" browser
		// that we import from rules_webtesting.
		// Note, the "REMOVE:" syntax is a feature of rules_webtesting, not
		// chrome.
		chromeArgs = append(chromeArgs, "REMOVE:--headless")
		// Add --verbose to chromedriver so that if it fails to start, we can
		// see the logs from Chrome with the root cause (e.g. missing system
		// deps, missing DISPLAY environment variable for X server, etc.)
		chromedriverArgs = append(chromedriverArgs, "--verbose")
	}

	capabilities := selenium.Capabilities{
		chrome.CapabilitiesKey: chrome.Capabilities{Args: chromeArgs},
		"google:wslConfig":     map[string]any{"args": chromedriverArgs},
	}
	driver, err := webtest.NewWebDriverSession(capabilities)
	require.NoError(t, err, "failed to create webdriver session")
	// Allow webdriver to wait a short period before giving up on finding an
	// element. In most cases, Selenium's default heuristics for marking a page
	// "ready" should prevent the need for this, but this implicit wait timeout
	// should give us enough wiggle room to avoid the need for any synchronization
	// logic between the app and the test. We should keep this very short to
	// avoid hiding real issues and to fail fast in the case where an element will
	// never be located.
	//
	// See also https://stackoverflow.com/q/11001030
	driver.SetImplicitWaitTimeout(*implicitWaitTimeout)
	wt := &WebTester{t, driver}
	t.Cleanup(func() {
		err := wt.screenshot("END_OF_TEST")
		// NOTE: `assert` here instead of `require` so that we still close down
		// the webdriver if the screenshot fails.
		assert.NoError(t, err, "failed to take end-of-test screenshot")

		if *debug && t.Failed() {
			time.Sleep(*endOfTestDelay)
		}
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

// Returns the current URL, fails if there is an error.
func (wt *WebTester) CurrentURL() string {
	url, err := wt.driver.CurrentURL()
	require.NoError(wt.t, err)
	return url
}

// Returns the <body> element of the current page. Exactly one body element
// must exist, otherwise the test fails.
func (wt *WebTester) FindBody() *Element {
	el, err := wt.driver.FindElement(selenium.ByTagName, "body")
	require.NoError(wt.t, err)
	return &Element{wt.t, el}
}

// Find returns the element matching the given CSS selector. Exactly one
// element must be matched, otherwise the test fails.
func (wt *WebTester) Find(cssSelector string) *Element {
	el, err := wt.driver.FindElement(selenium.ByCSSSelector, cssSelector)
	require.NoError(wt.t, err)
	return &Element{wt.t, el}
}

// FindAll returns all elements matching the given CSS selector.
func (wt *WebTester) FindAll(cssSelector string) []*Element {
	els, err := wt.driver.FindElements(selenium.ByCSSSelector, cssSelector)
	require.NoError(wt.t, err)
	out := make([]*Element, len(els))
	for i, el := range els {
		out[i] = &Element{wt.t, el}
	}
	return out
}

// FindByDebugID returns the element matching the given debug-id selector. Exactly one
// element must be matched, otherwise the test fails.
//
// It's recommended to use the debug-id attribute for webdriver tests. Using CSS names, IDs, or classes
// can be brittle when element style is changed. debug-ids are only used for webdriver tests and should be stable
func (wt *WebTester) FindByDebugID(debugID string) *Element {
	el, err := wt.driver.FindElement(selenium.ByCSSSelector, `[debug-id="`+debugID+`"]`)
	require.NoError(wt.t, err)
	return &Element{wt.t, el}
}

// AssertNotFound asserts the provided CSS selector does not exist in the DOM.
func (wt *WebTester) AssertNotFound(cssSelector string) {
	els, err := wt.driver.FindElements(selenium.ByCSSSelector, cssSelector)
	require.NoError(wt.t, err)
	require.Empty(wt.t, els)
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
	screenshotFile, err := os.CreateTemp(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), fmt.Sprintf("%s.%s.screenshot-*.png", wt.t.Name(), tag))
	if err != nil {
		return err
	}
	defer screenshotFile.Close()

	sc, err := wt.driver.Screenshot()
	if err != nil {
		return err
	}
	if _, err := screenshotFile.Write(sc); err != nil {
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

// IsSelected returns whether the element is selected.
func (el *Element) IsSelected() bool {
	val, err := el.webElement.IsSelected()
	require.NoError(el.t, err)
	return val
}

// GetAttribute returns the named attribute of the element.
func (el *Element) GetAttribute(name string) string {
	val, err := el.webElement.GetAttribute(name)
	require.NoError(el.t, err)
	return val
}

// SendKeys types into the element.
func (el *Element) SendKeys(keys string) {
	err := el.webElement.SendKeys(keys)
	require.NoError(el.t, err)
}

// Find returns the child element matching the given CSS selector. Exactly one
// element must be matched, otherwise the test fails.
func (el *Element) Find(cssSelector string) *Element {
	child, err := el.webElement.FindElement(selenium.ByCSSSelector, cssSelector)
	require.NoError(el.t, err)
	return &Element{t: el.t, webElement: child}
}

func (el *Element) FirstSelectedOption() *Element {
	options, err := el.webElement.FindElements(selenium.ByCSSSelector, "option")
	require.NoError(el.t, err)
	for _, option := range options {
		selected, err := option.IsSelected()
		require.NoError(el.t, err)
		if selected {
			return &Element{t: el.t, webElement: option}
		}
	}
	require.FailNow(el.t, "no options were selected")
	return nil
}

// ===
// Utility functions that don't directly correspond with WebElement APIs
// ===

// HasClass returns whether an element has the given class name.
func HasClass(el *Element, class string) bool {
	classes := strings.Split(el.GetAttribute("class"), " ")
	for _, c := range classes {
		if c == class {
			return true
		}
	}
	return false
}

// ===
// BuildBuddy-specific functionality
// ===

type Target interface {
	// HTTPURL is the HTTP endpoint of the target.
	// Ex: http://localhost:8080
	HTTPURL() string
}

type SSOTarget interface {
	Target
	// SSOSlug is the slug of the org that should be used for SSO login.
	SSOSlug() string
}

func Login(wt *WebTester, target Target) {
	wt.Get(target.HTTPURL())

	// If the target supports SSO login, prefer that.
	if target, ok := target.(SSOTarget); ok {
		wt.FindByDebugID("sso-button").Click()
		wt.FindByDebugID("sso-slug").SendKeys(target.SSOSlug())
		wt.FindByDebugID("sso-button").Click()
	} else {
		// Otherwise attempt self-auth.
		wt.FindByDebugID("login-button").Click()
	}

	// Login can have a delay. Wait for the sidebar before proceeding.
	wt.Find(".sidebar-footer")
}

// Logout logs out of the app. It expects that a user is currently logged in,
// with the sidebar visible in the UI.
func Logout(wt *WebTester) {
	ExpandSidebarOptions(wt)
	wt.Find(".sidebar-logout-item").Click()
}

// ExpandSidebarOptions expands the sidebar options, exposing the section
// containing links to Logout, Settings, etc.
func ExpandSidebarOptions(wt *WebTester) {
	toggle := wt.Find(".sidebar-footer")
	if HasClass(toggle, "expanded") {
		return
	}
	toggle.Click()
}

// ClickSidebarItem clicks the sidebar item with the given label.
func ClickSidebarItem(wt *WebTester, label string) {
	items := wt.FindAll(".sidebar-item")
	for _, item := range items {
		if strings.TrimSpace(item.Text()) == label {
			item.Click()
			return
		}
	}
	require.FailNowf(wt.t, "Failed to locate sidebar item", "label: %s", label)
}

// SetupPageOption applies a desired setting to the setup page.
type SetupPageOption func(wt *WebTester)

var (
	// WithEnableCache is a setup page option that checks the "enable cache"
	// checkbox.
	WithEnableCache SetupPageOption = func(wt *WebTester) {
		enableCacheCheckbox := wt.Find("#cache")
		if !enableCacheCheckbox.IsSelected() {
			enableCacheCheckbox.Click()
		}

		fullCacheRadioButton := wt.Find("#cache-full")
		if !fullCacheRadioButton.IsSelected() {
			fullCacheRadioButton.Click()
		}
	}
)

// WithAPIKeySelection returns a setup page option that selects a given API key
// from the dropdown.
func WithAPIKeySelection(label string) SetupPageOption {
	return func(wt *WebTester) {
		picker := wt.Find(`.credential-picker`)
		picker.SendKeys(label)
		selectedOption := picker.FirstSelectedOption()
		require.Equal(wt.t, label, selectedOption.Text())
	}
}

// GetBazelBuildFlags uses the Web app to navigate to the setup page and get the
// Bazel config flags recommended by BuildBuddy. Options can be passed to select
// config options via the UI.
func GetBazelBuildFlags(wt *WebTester, appBaseURL string, opts ...SetupPageOption) []string {
	wt.Get(appBaseURL + "/docs/setup/")
	for _, fn := range opts {
		fn(wt)
	}
	rawBazelrc := wt.Find(`[data-header=".bazelrc"] .contents`).Text()
	lines := strings.Split(strings.TrimSpace(rawBazelrc), "\n")
	buildFlags := make([]string, 0, len(lines))
	for _, line := range lines {
		// Remove comments
		parts := strings.Split(line, "#")
		if len(parts) > 1 {
			line = parts[0]
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "build ") {
			buildFlags = append(buildFlags, strings.TrimPrefix(line, "build "))
		}
	}
	return buildFlags
}

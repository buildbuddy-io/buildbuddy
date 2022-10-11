package webtester

import (
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
	// Allow webdriver to wait a short period before giving up on finding an
	// element. In most cases, Selenium's default heuristics for marking a page
	// "ready" should prevent the need for this, but this implicit wait timeout
	// should give us enough wiggle room to avoid the need for any synchronization
	// logic between the app and the test. We should keep this very short to
	// avoid hiding real issues and to fail fast in the case where an element will
	// never be located.
	//
	// See also https://stackoverflow.com/q/11001030
	driver.SetImplicitWaitTimeout(1 * time.Second)
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

// Login uses the Web app to log into BuildBuddy as the default self-auth user.
// It expects that no user is currently logged in, and that self-auth is
// enabled.
func Login(wt *WebTester, appBaseURL string) {
	wt.Get(appBaseURL)
	wt.Find(".login-button").Click()
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
		checkbox := wt.Find("#cache")
		if !checkbox.IsSelected() {
			checkbox.Click()
		}
	}
)

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

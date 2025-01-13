package webtester

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_webtesting/go/webtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"

	selenium_log "github.com/tebeka/selenium/log"
)

var (
	// To debug webdriver tests visually in your local environment:
	//
	// 1. Run the test with --config=webdriver-debug, which causes the test
	//    browser to be displayed while the test is running.
	//
	// 2. Optionally, set --test_arg=--webdriver_end_of_test_delay=1h to extend
	//    the end-of-test wait duration so that you have enough time to look at
	//    devtools etc. at the moment the test fails.
	//
	// 3. Optionally, set --test_filter=NameOfTest to debug only a single test.
	//
	// 4. Optionally, set --test_arg=--webdriver_verbose for more verbose logs.
	//    This can be useful for trying to understand what the webdriver is
	//    doing under the hood, as well as debugging browser crashes.

	headless            = flag.Bool("webdriver_headless", true, "Run webdriver in headless mode (no GUI). Can be set to false for local debugging.")
	verbose             = flag.Bool("webdriver_verbose", false, "Show verbose logs from the browser.")
	endOfTestDelay      = flag.Duration("webdriver_end_of_test_delay", 0, "How long to wait at the end of failed webdriver tests. Useful in combination with -webdriver_headless=false, so that the browser window doesn't immediately close upon failure.")
	implicitWaitTimeout = flag.Duration("webdriver_implicit_wait_timeout", 1*time.Second, "Max time webtester should wait to find an element.")
)

// ShortcutModifierKey is the modifier key for most shortcuts such as
// cut/copy/paste: Control on Windows/Linux or Command on macOS.
var ShortcutModifierKey string

func init() {
	if runtime.GOOS == "darwin" {
		ShortcutModifierKey = selenium.MetaKey
	} else {
		ShortcutModifierKey = selenium.ControlKey
	}
}

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
		// When these tests are run inside containers on RBE, the size of
		// /dev/shm is limited to 64MB, which is not enough for chrome, and can
		// cause page crashes. Disable /dev/shm usage to fix this.
		// See https://stackoverflow.com/questions/53902507/unknown-error-session-deleted-because-of-page-crash-from-unknown-error-cannot/53970825#53970825
		"--disable-dev-shm-usage",
	}
	chromedriverArgs := []string{}
	if !*headless {
		// Remove the --headless arg applied to the "chromium-local" browser
		// that we import from rules_webtesting.
		// Note, the "REMOVE:" syntax is a feature of rules_webtesting, not
		// chrome.
		chromeArgs = append(chromeArgs, "REMOVE:--headless")
	}
	if *verbose {
		// Add --verbose to chromedriver so that if it fails to start, we can
		// see the logs from Chrome with the root cause (e.g. missing system
		// deps, missing DISPLAY environment variable for X server, etc.)
		chromedriverArgs = append(chromedriverArgs, "--verbose")
	}

	capabilities := selenium.Capabilities{
		chrome.CapabilitiesKey: chrome.Capabilities{Args: chromeArgs},
		"google:wslConfig":     map[string]any{"args": chromedriverArgs},
	}
	// Set the log level so that we can retrieve everything that was printed
	// to the console.
	capabilities.SetLogLevel(selenium_log.Browser, selenium_log.All)

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
		assertErrorBannerNeverShown(wt)

		err := wt.screenshot("END_OF_TEST")
		// NOTE: `assert` here instead of `require` so that we still close down
		// the webdriver if the screenshot fails.
		assert.NoError(t, err, "failed to take end-of-test screenshot")

		if *endOfTestDelay > 0 {
			t.Logf("Sleeping for %s (-webdriver_end_of_test_delay)", *endOfTestDelay)
			time.Sleep(*endOfTestDelay)
		}
		err = driver.Quit()
		require.NoError(t, err)
	})
	return wt
}

// LogString returns the browser's console logs as a flat string.
// This can be useful for checking that certain events did not occur, which
// may otherwise be difficult to test using only the UI.
func (wt *WebTester) LogString() string {
	var builder strings.Builder
	logs, err := wt.driver.Log(selenium_log.Browser)
	require.NoError(wt.t, err)
	for _, log := range logs {
		builder.WriteString(fmt.Sprintf("[%s] %s\n", log.Level, log.Message))
	}
	return builder.String()
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

// Refresh reloads the page.
func (wt *WebTester) Refresh() {
	wt.Get(wt.CurrentURL())
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
//
// FindAll does not wait for elements matching the selector to be created, since
// it's valid for the selector to match 0 elements.
//
// NOTE: Because FindAll() does not wait for elements matching the selector to
// be created, locating a list of items requires two steps. First locate the
// parent list container using Find(), which will wait for the list to be
// created. Then, call FindAll() with a selector matching the list items.
// Example:
//
//	for _, item := range wt.Find(".list").FindAll(".list-item") {
//		// ...
//	}
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

// SetChecked sets whether a checkbox is checked or not. It returns whether the
// state was changed. It fails the test if the checkbox did not respond to the
// click.
func (el *Element) SetChecked(checked bool) (changed bool) {
	if el.IsSelected() != checked {
		el.Click()
		if el.IsSelected() != checked {
			require.FailNow(el.t, "checkbox state did not update after click")
		}
		return true
	}
	return false
}

// GetAttribute returns the named attribute of the element.
func (el *Element) GetAttribute(name string) string {
	val, err := el.webElement.GetAttribute(name)
	require.NoError(el.t, err)
	return val
}

// Clear clears the element.
func (el *Element) Clear() {
	// el.webElement.Clear() appears to be unreliable in some cases - see
	// https://stackoverflow.com/questions/50677760/selenium-clear-command-doesnt-clear-the-element
	// https://github.com/SeleniumHQ/selenium/issues/11739
	//
	// Instead, select all text and delete it.
	el.SendKeys(ShortcutModifierKey + "a")
	el.SendKeys(selenium.DeleteKey)
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

// FindAll returns all elements matching the given CSS selector.
//
// FindAll does not wait for elements matching the selector to be created, since
// it's valid for the selector to match 0 elements.
func (el *Element) FindAll(cssSelector string) []*Element {
	els, err := el.webElement.FindElements(selenium.ByCSSSelector, cssSelector)
	require.NoError(el.t, err)
	out := make([]*Element, len(els))
	for i, c := range els {
		out[i] = &Element{el.t, c}
	}
	return out
}

func (el *Element) FirstSelectedOption() *Element {
	options, err := el.webElement.FindElements(selenium.ByCSSSelector, "option")
	require.NoError(el.t, err)
	for _, option := range options {
		selected, err := option.IsSelected()
		require.NoError(el.t, err)
		if selected {
			return &Element{el.t, option}
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
	wt.FindByDebugID("org-picker")
}

// Logout logs out of the app. It expects that a user is currently logged in,
// with the sidebar visible in the UI.
func Logout(wt *WebTester) {
	ExpandSidebarOptions(wt)
	wt.FindByDebugID("logout-button").Click()
}

// Fails the test if the error banner was shown at any point during the test.
func assertErrorBannerNeverShown(wt *WebTester) {
	logs := wt.LogString()
	// TODO(bduffany): fix error banner displayed for these tests and remove
	// this check.
	if name := wt.t.Name(); name == "TestAuthenticatedInvocation_CacheEnabled" ||
		name == "TestAuthenticatedInvocation_PersonalAPIKey_CacheEnabled" ||
		name == "TestSAMLLogin" {
		return
	}
	if strings.Contains(logs, "Displaying error banner") {
		assert.Fail(wt.t, "Error banner was displayed during test", "%s", logs)
	}
}

// ExpandSidebarOptions expands the sidebar options, exposing the section
// containing links to Logout, Settings, etc.
func ExpandSidebarOptions(wt *WebTester) {
	toggle := wt.FindByDebugID("org-picker")
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

	// WithEnableRemoteExecution is a setup page option that checks the "enable
	// remote execution" checkbox.
	WithEnableRemoteExecution SetupPageOption = func(wt *WebTester) {
		checkbox := wt.Find("#execution")
		if !checkbox.IsSelected() {
			checkbox.Click()
		}
	}
)

// WithAPIKeySelection returns a setup page option that selects a given API key
// from the dropdown.
func WithAPIKeySelection(label string) SetupPageOption {
	return func(wt *WebTester) {
		picker := wt.Find(`.credential-picker`)
		picker.SendKeys(label)
		picker = wt.Find(`.credential-picker`)
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

// OrgOption controls how the org form is filled out (create or update).
type OrgOption func(wt *WebTester)

var EnableUserOwnedAPIKeys OrgOption = func(wt *WebTester) {
	wt.Find(`[name="userOwnedKeysEnabled"]`).SetChecked(true)
}

// UpdateSelectedOrg updates org details for the currently selected org.
func UpdateSelectedOrg(wt *WebTester, appBaseURL, name, slug string, options ...OrgOption) {
	wt.Get(appBaseURL + "/settings/org/details")
	SubmitOrgForm(wt, name, slug, options...)
	wt.Find(`.form-success-message`)
}

// CreateOrg creates a new BuildBuddy organization with the given settings.
func CreateOrg(wt *WebTester, appBaseURL, name, slug string, options ...OrgOption) {
	wt.Get(appBaseURL + "/org/create")
	SubmitOrgForm(wt, name, slug, options...)
}

// SubmitOrgForm fills in the org form with the given details and submits it.
func SubmitOrgForm(wt *WebTester, name, slug string, options ...OrgOption) {
	nameField := wt.Find(`[name="name"]`)
	nameField.Clear()
	nameField.SendKeys(name)
	slugField := wt.Find(`[name="urlIdentifier"]`)
	slugField.Clear()
	slugField.SendKeys(slug)
	for _, option := range options {
		option(wt)
	}
	wt.Find(`.organization-form-submit-button`).Click()
}

// LeaveSelectedOrg removes the currently logged in user from whichever org
// they've selected. It fails the test if no user is logged in or the logged
// in user is not a member if any org.
func LeaveSelectedOrg(wt *WebTester, appBaseURL string) {
	wt.Get(appBaseURL + "/settings/org/members")
	for _, listItem := range wt.Find(`.org-members-list`).FindAll(`.org-members-list-item`) {
		if strings.Contains(listItem.Text(), "(You)") {
			listItem.Click()
			wt.Find(`.org-member-remove-button`).Click()
			wt.Find(`.org-members-edit-modal button.destructive`).Click()
			return
		}
	}
	require.FailNow(wt.t, "could not find org member list item labeled with '(You)'")
}

func GetOrCreatePersonalAPIKey(wt *WebTester, appBaseURL string) string {
	wt.Get(appBaseURL + "/settings/personal/api-keys")
	existingKeys := wt.FindAll(`.api-key-value`)
	if len(existingKeys) == 0 {
		wt.FindByDebugID("create-new-api-key").Click()
		wt.Find(`.dialog-wrapper [name="label"]`).SendKeys("test-personal-key")
		wt.FindByDebugID("cas-only-radio-button").Click()
		wt.Find(`.dialog-wrapper button[type="submit"]`).Click()
	}
	wt.Find(`.api-key-value-hide`).Click()
	apiKey := ""
	for i := 0; i < 5; i++ {
		apiKey = wt.Find(".api-key-value").Text()
		// Wait for the API key value to load
		if !strings.Contains(apiKey, "••••") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotContains(wt.t, apiKey, "••••")
	return apiKey
}

import "./auth_service_test_setup";

import { AuthService } from "./auth_service";
import { testGlobal } from "./auth_service_test_setup";

describe("AuthService.handleTokenRefreshError", () => {
  beforeEach(() => {
    testGlobal.localStorage.clear();
    testGlobal.window.location.href = "https://app.example.com/invocation/123";
  });

  it("uses the SPA login page when no OIDC providers are configured", () => {
    const authService = new AuthService();
    authService.handleTokenRefreshError();

    expect(testGlobal.localStorage.getItem("auto_login_attempted")).toBe("true");
    expect(testGlobal.window.location.href).toBe(
      `/?${new URLSearchParams({
        redirect_url: "https://app.example.com/invocation/123",
      })}`
    );
  });
});

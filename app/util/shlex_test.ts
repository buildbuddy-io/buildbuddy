import { quote } from "./shlex";

describe("shlex", () => {
  it("should quote arguments", () => {
    expect(quote("foo")).toBe("foo");
    expect(quote("--path=C:\\Program Files")).toBe("--path='C:\\Program Files'");
    expect(quote("foo bar")).toBe("'foo bar'");
    expect(quote("--foo=bar")).toBe("--foo=bar");
    expect(quote("--bes_backend=grpc://localhost:1985")).toBe("--bes_backend=grpc://localhost:1985");
    expect(quote("-@//foo/...")).toBe("-@//foo/...");
  });
});

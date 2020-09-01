export enum IconType {
  Default,
  Success,
  Failure,
  InProgress,
  Unknown,
}

class FaviconService {
  getDefaultFavicon() {
    if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)")?.matches) {
      return "/favicon/favicon_white.svg";
    }
    return "/favicon/favicon_black.svg";
  }

  getFaviconForType(type: IconType) {
    switch (type) {
      case IconType.Success:
        return "/favicon/favicon_green.svg";
      case IconType.Failure:
        return "/favicon/favicon_red.svg";
      case IconType.InProgress:
        return "/favicon/favicon_blue.svg";
      case IconType.Unknown:
        return "/favicon/favicon_grey.svg";
      default:
        return this.getDefaultFavicon();
    }
  }

  setFaviconForType(type: IconType) {
    document.getElementById("favicon")?.setAttribute("href", this.getFaviconForType(type));
  }

  setDefaultFavicon() {
    this.setFaviconForType(IconType.Default);
  }
}

export default new FaviconService();

class Router {
  register(pathChangeHandler: VoidFunction) {
    history.pushState = (f => function pushState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.pushState);

    history.replaceState = (f => function replaceState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.replaceState);

    window.addEventListener('popstate', () => {
      pathChangeHandler();
    });
  }

  updateParams(params: any) {
    let keys = Object.keys(params);
    let queryParam = keys.map(key => `${key}=${params[key]}`).join('&');
    var newurl = window.location.protocol + "//" + window.location.host + window.location.pathname + queryParam + window.location.hash;
    window.history.pushState({ path: newurl }, '', newurl);
  }

  getInvocationId(path: string) {
    let invocationPath = "/invocation/"
    if (!path.startsWith(invocationPath)) {
      return null;
    }
    return path.replace(invocationPath, "");
  }
}

export default new Router();
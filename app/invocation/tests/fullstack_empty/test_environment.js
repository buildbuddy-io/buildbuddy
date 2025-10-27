const HTTP_PORT = 19080;

module.exports = {
  HTTP_PORT,
  HEALTH_URL: `http://127.0.0.1:${HTTP_PORT}/healthz?server-type=buildbuddy-server`,
};

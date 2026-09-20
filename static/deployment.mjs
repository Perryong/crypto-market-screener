export function backendURLs(value, origin) {
  const page = new URL(origin);
  if (!value && page.hostname.endsWith('.github.io')) {
    throw new Error('Backend not configured. Set the CRYEXC_API_BASE GitHub repository variable to your public HTTPS backend URL and redeploy.');
  }
  const base = new URL(value || origin);
  if (!['http:', 'https:'].includes(base.protocol)) throw new Error('Backend must use HTTP or HTTPS');
  if (page.protocol === 'https:' && base.protocol !== 'https:') throw new Error('An HTTPS dashboard requires an HTTPS backend');
  if (base.username || base.password || base.search || base.hash) throw new Error('Backend URL must not contain credentials, query parameters or fragments');
  const http = base.href.replace(/\/$/, '');
  return {http, ws: http.replace(/^http/, 'ws') + '/ws'};
}

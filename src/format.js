// WordPress often sends HTML. WhatsApp does not render HTML, so convert it
// to WhatsApp formatting (*bold*, _italic_) and plain links.

export function toWhatsAppText(input) {
  let s = String(input ?? '');

  s = s.replace(/<a\s+[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi, (_, href, label) => {
    const text = label.replace(/<[^>]+>/g, '').trim();
    return text && text !== href ? `${text}: ${href}` : href;
  });
  s = s.replace(/<\s*(b|strong)\s*>([\s\S]*?)<\/\s*\1\s*>/gi, '*$2*');
  s = s.replace(/<\s*(i|em)\s*>([\s\S]*?)<\/\s*\1\s*>/gi, '_$2_');
  s = s.replace(/<\s*br\s*\/?>/gi, '\n');
  s = s.replace(/<\s*li[^>]*>/gi, '• ');
  s = s.replace(/<\/\s*(p|div|li|h[1-6]|tr|ul|ol)\s*>/gi, '\n');
  s = s.replace(/<[^>]+>/g, '');

  s = s
    .replace(/&nbsp;/g, ' ')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#0?39;/g, "'")
    .replace(/&amp;/g, '&');

  return s.replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

// "0412 345 678" -> "61412345678"; "+61 412..." -> "61412..."; "6141...@c.us" -> "6141..."
export function normalizePhone(raw, defaultCountryCode = '') {
  let d = String(raw ?? '').replace(/@.*$/, '').replace(/[^\d]/g, '');
  if (!d) return null;
  if (d.startsWith('00')) d = d.slice(2);
  else if (d.startsWith('0') && defaultCountryCode) d = defaultCountryCode + d.slice(1);
  return d.length >= 8 && d.length <= 15 ? d : null;
}

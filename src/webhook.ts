import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Verify the `x-pmw-signature` header of a webhook delivery: hex HMAC-SHA256 of the RAW request body,
 * keyed with the secret you were shown when you registered the endpoint.
 *
 * Pass the raw bytes, before any JSON parsing — re-serialising changes them and every check fails.
 */
export function verifyWebhook(rawBody: string | Uint8Array, signatureHex: string | undefined | null, secret: string): boolean {
  if (!signatureHex || !secret) return false;
  if (!/^[0-9a-fA-F]+$/.test(signatureHex)) return false;
  const sent = Buffer.from(signatureHex, 'hex');
  const mine = createHmac('sha256', secret).update(rawBody).digest();
  return sent.length === mine.length && timingSafeEqual(sent, mine);
}

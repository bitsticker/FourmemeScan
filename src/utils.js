'use strict';
/**
 * utils.js — 公共工具函数模块
 *
 * 所有模块共用的纯函数集中在此，避免重复定义：
 *   - formatBeijingTimeMs: 格式化为北京时间（含毫秒）
 *   - parseBeijingTime:    解析北京时间字符串为 Date
 *   - extractTweetId:      从 URL 提取 tweet ID
 *   - toReadableTwitter:   URL → {display, href, username}
 *   - TWITTER_EPOCH:       Snowflake 时间基准
 */

// Twitter Snowflake 时间基准（用于从 tweet ID 推算发推时间）
const TWITTER_EPOCH = 1288834974657n;

/**
 * 格式化 Date 为北京时间字符串（含毫秒）
 * 输出: "MM/DD HH:MM:SS.mmm"
 */
function formatBeijingTimeMs(date) {
  const base = date.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
  const noYear = base.replace(/^\d{4}\//, '');
  return noYear + '.' + String(date.getMilliseconds()).padStart(3, '0');
}

/**
 * 将 formatBeijingTimeMs 的输出解析回时间戳（ms）
 * 用于两个同源时间点做差值比较
 * 解析失败返回 null
 */
function parseBeijingTime(str) {
  if (!str) return null;
  try {
    let s = String(str).trim();
    let year;
    const yearMatch = s.match(/^(\d{4})\//);
    if (yearMatch) {
      year = Number(yearMatch[1]);
      s = s.slice(yearMatch[0].length);
    } else {
      year = new Date().getFullYear();
    }
    const m = s.match(/^(\d{1,2})\/(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d{1,3}))?/);
    if (!m) {
      const t = Date.parse(str);
      return isNaN(t) ? null : t;
    }
    const d = new Date(year, Number(m[1]) - 1, Number(m[2]), Number(m[3]), Number(m[4]), Number(m[5]), m[6] ? Number(m[6].padEnd(3, '0')) : 0);
    return d.getTime();
  } catch (_) {
    return null;
  }
}

/**
 * 从 Twitter/X URL 中提取 tweet ID
 * 支持: x.com/user/status/123, twitter.com/i/status/123 等
 */
function extractTweetId(url) {
  if (!url) return null;
  const m = url.match(/\/status\/(\d+)/i) || url.match(/\/(\d{15,20})/);
  return m ? m[1] : null;
}

/**
 * 将 twitter/x URL 转为可读格式
 * 返回 { display: '@username', href: '完整URL', username: 'username' }
 */
function toReadableTwitter(url) {
  if (!url) return { display: '', href: '', username: '' };
  try {
    const statusMatch = url.match(/(?:twitter\.com|x\.com)\/([^/?#\s]+)\/status\/(\d+)/i);
    if (statusMatch) {
      const username = statusMatch[1];
      return { display: '@' + username, href: `https://x.com/${username}/status/${statusMatch[2]}`, username };
    }
    const userMatch = url.match(/(?:twitter\.com|x\.com)\/([^/?#\s]+)/i);
    if (userMatch) {
      const username = userMatch[1];
      return { display: '@' + username, href: `https://x.com/${username}`, username };
    }
    if (url.startsWith('/')) {
      const parts = url.split('/').filter(Boolean);
      const username = parts[0];
      if (parts.length >= 3 && parts[1] === 'status') {
        return { display: '@' + username, href: `https://x.com/${username}/status/${parts[2]}`, username };
      }
      return { display: '@' + username, href: `https://x.com/${username}`, username };
    }
    if (url.startsWith('@')) {
      const username = url.slice(1).split('/')[0].split('?')[0];
      return { display: url, href: `https://x.com/${username}`, username };
    }
  } catch (_) {}
  return { display: url, href: url.startsWith('http') ? url : 'https://' + url, username: '' };
}

module.exports = {
  TWITTER_EPOCH,
  formatBeijingTimeMs,
  parseBeijingTime,
  extractTweetId,
  toReadableTwitter,
};

# MyTube Telegram Bot

ভিডিও ফরোয়ার্ড করলে বট সিরিয়াল অনুযায়ী চ্যানেলে রাখে, ডাইরেক্ট লিংক বানায়, আর MyTube অ্যাপ `/api/videos` থেকে লিস্ট নেয়।

Render (Docker) এ deploy করুন। Environment variables: `_.env.example` দেখুন।
- `/api/videos?after=ID` — ভিডিও লিস্ট (Header: `X-API-Key`)
- `/api/backup` GET/POST — অ্যাপের ডাটা ব্যাকআপ (চ্যানেলে JSON ফাইল হিসেবে)
- `/thumb/ID/HASH`, `/stream/ID/HASH`, `/dl/ID/HASH`

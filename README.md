# 🕵️ Нейромайор

Шуточный Telegram-бот, который с нескрываемым удовольствием следит за перепиской участников чата, штрафует их по статьям УК РФ и ведёт личное досье на каждого — исключительно в воспитательных целях, разумеется.

## Команды

| Команда | Описание |
|---|---|
| `/stats` | Аудит сообщений на нарушения, сводный реестр с индексом патриотичности |
| `/case @username` | Полное досье нарушений конкретного гражданина |
| `/profile @username` | Психологический портрет в стиле аналитика КГБ |
| `/summary` | Саркастичный пересказ последних 100 сообщений чата |
| `/stats_reset` | Амнистия — очистка всех нарушений (только для администраторов) |

## Требования

- Docker и Docker Compose
- Telegram Bot Token ([@BotFather](https://t.me/BotFather))
- Gemini API Key ([Google AI Studio](https://aistudio.google.com))

## Запуск

1. Клонируй репозиторий:
```bash
git clone https://github.com/github-ninja/neromajor.git
cd neromajor
```

2. Создай `.env` на основе `.env.example` и заполни переменные.

3. Запусти:
```bash
docker compose up -d
```

## Логика подсчёта нарушений

Подробное описание — [в этой статье](https://telegra.ph/Nejromajor--logika-podscheta-narushenij-02-27).

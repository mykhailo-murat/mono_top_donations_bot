# main.py
import logging
import pandas as pd
from telegram import Update, Document, InputFile
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes,
    filters,
)
from io import BytesIO, StringIO
import csv

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = "7870393276:AAFJJMETllErbVSomsPgkcJur2xwvmDhutE"

# Handle CSV documents
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document: Document = update.message.document
    file = await context.bot.get_file(document.file_id)
    file_content = await file.download_as_bytearray()

    try:
        decoded = file_content.decode("utf-8")
        lines = decoded.splitlines()
        good_lines = []
        bad_lines = []

        reader = csv.reader(lines, quotechar='"', escapechar='\\')
        for i, row in enumerate(reader, start=1):
            if len(row) == 8:
                good_lines.append(row)
            else:
                bad_lines.append((i, lines[i-1]))

        df = pd.DataFrame(good_lines[1:], columns=good_lines[0])

        df = df[df["Додаткова інформація"].str.contains("Від:", na=False)].copy()
        df["Донатор"] = df["Додаткова інформація"].str.extract(r"Від:\s*(.+)")

        summary = (
            df.groupby("Донатор")["Сума"]
            .astype(float)
            .agg(['sum', 'count'])
            .sort_values(by="sum", ascending=False)
        )
        summary.reset_index(inplace=True)
        summary.columns = ["Ім’я", "Сума (грн)", "Кількість поповнень"]

        text = "\U0001F4B0 <b>ТОП донатери</b>\n\n"
        for idx, row in summary.iterrows():
            text += f"<b>{idx+1}. {row['Ім’я']}</b> — {row['Сума (грн)']} грн ({row['Кількість поповнень']} разів)\n"

        if bad_lines:
            text += f"\n⚠️ Пропущено {len(bad_lines)} рядків через помилки структури:\n"
            preview = '\n'.join([f"{i}: {line}" for i, line in bad_lines[:5]])
            text += preview
            if len(bad_lines) > 5:
                text += f"\n...та ще {len(bad_lines) - 5} рядків."

            # Додаємо як текстовий файл усі погані рядки
            bad_output = '\n'.join([f"{i}: {line}" for i, line in bad_lines])
            bad_file = BytesIO(bad_output.encode("utf-8"))
            bad_file.name = "bad_rows.txt"
            await update.message.reply_document(document=InputFile(bad_file))

        await update.message.reply_text(text, parse_mode='HTML')

    except Exception as e:
        logging.exception("Помилка при обробці файлу")
        await update.message.reply_text("❌ Виникла помилка при обробці файлу. Перевірте його структуру.")


# Handle any other message
async def handle_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Надішліть, будь ласка, .CSV-файл звіту від Mono для аналізу донатів.")


if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    # CSV handler (ловить будь-які документи, далі — фільтрація у функції)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Fallback для всього іншого
    app.add_handler(MessageHandler(filters.ALL, handle_fallback))

    print("Бот запущено...")
    app.run_polling()

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
from io import BytesIO

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
        # Estimate skipped lines by comparing total lines to parsed rows
        decoded = file_content.decode("utf-8")
        total_lines = decoded.count("\n")

        df = pd.read_csv(
            BytesIO(file_content),
            quotechar='"',
            escapechar='\\',
            on_bad_lines='warn'
        )

        skipped_rows = total_lines - len(df)

        df = df[df["Додаткова інформація"].str.contains("Від:", na=False)].copy()
        df["Донатор"] = df["Додаткова інформація"].str.extract(r"Від:\s*(.+)")

        df["Сума"] = df["Сума"].astype(float)
        summary = (
            df.groupby("Донатор")["Сума"]
            .agg(['sum', 'count'])
            .sort_values(by="sum", ascending=False)
        )
        summary.reset_index(inplace=True)
        summary.columns = ["Ім’я", "Сума (грн)", "Кількість поповнень"]

        text = "\U0001F4B0 <b>ТОП донатери</b>\n\n"
        for idx, row in summary.iterrows():
            text += f"<b>{idx+1}. {row['Ім’я']}</b> — {row['Сума (грн)']} грн ({row['Кількість поповнень']} разів)\n"

        if skipped_rows > 0:
            text += f"\n⚠️ Пропущено рядків через помилки: {skipped_rows}\n"

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
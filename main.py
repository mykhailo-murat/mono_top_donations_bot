# main.py
import logging
import pandas as pd
from telegram import Update, Document
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from io import BytesIO


# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = "7870393276:AAFJJMETllErbVSomsPgkcJur2xwvmDhutE"

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document: Document = update.message.document
    print("Отримано документ:", document.file_name)

    await update.message.reply_text(f"Отримано файл: {document.file_name}")

    if not document.file_name.endswith(".csv"):
        await update.message.reply_text("Будь ласка, надішліть файл у форматі CSV.")
        return

    file = await context.bot.get_file(document.file_id)
    file_content = await file.download_as_bytearray()

    try:
        df = pd.read_csv(BytesIO(file_content))

        # Фільтруємо лише поповнення "Від:"
        df = df[df["Додаткова інформація"].str.contains("Від:", na=False)].copy()
        df["Донатор"] = df["Додаткова інформація"].str.extract(r"Від:\s*(.+)")

        # Підсумок по донаторах
        summary = (
            df.groupby("Донатор")["Сума"]
            .agg(['sum', 'count'])
            .sort_values(by="sum", ascending=False)
        )
        summary.reset_index(inplace=True)
        summary.columns = ["Ім’я", "Сума (грн)", "Кількість поповнень"]

        # Формуємо відповідь
        text = "\U0001F4B0 <b>ТОП донатери</b>\n\n"
        for idx, row in summary.iterrows():
            text += f"<b>{idx+1}. {row['Ім’я']}</b> — {row['Сума (грн)']} грн ({row['Кількість поповнень']} разів)\n"

        await update.message.reply_text(text, parse_mode='HTML')

    except Exception as e:
        logging.exception("Помилка при обробці файлу")
        await update.message.reply_text("Виникла помилка при обробці файлу. Перевірте його структуру.")


if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    print("Бот запущено...")
    app.run_polling()

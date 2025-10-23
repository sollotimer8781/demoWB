# demoWB — управление ассортиментом маркетплейсов

Единое Streamlit-приложение для работы с ассортиментом Wildberries, Ozon и SBIS. Репозиторий собран так, чтобы запуститься «из коробки» и быстро подключаться к внешним сервисам и базам данных.

## Ключевые возможности

- 📦 **Каталог товаров** на SQLAlchemy (Postgres/SQLite) с поддержкой импортов, экспортов и пользовательских колонок.
- 🔄 **Синхронизация маркетплейсов**: отдельные страницы для Ozon и Wildberries, работающие через Streamlit secrets.
- 📊 **Data Workspace** — рабочее место коэффициентов и аналитики по данным из `product_items`.
- 🗂️ **Единая точка входа `streamlit_app.py`**: общая шапка, навигация и onboarding с подсказками по конфигурации.
- 🛠️ **Alembic** для миграций, модуль `db.py` с поддержкой `DATABASE_URL` и fallback на `db.sqlite3`.
- ⚙️ **Конфигурация через `.env` и `.streamlit/secrets.toml`**, подключена поддержка `python-dotenv`.

## Структура репозитория

```
/home/engine/project
├── streamlit_app.py            # Главная страница и onboarding
├── app_layout.py               # Инициализация Streamlit, шапка и навигация
├── pages/
│   ├── 1_Products.py           # Каталог товаров (SQLAlchemy)
│   ├── 2_OZON_Products.py      # Ozon Seller API
│   ├── WB_Products.py          # Wildberries API
│   ├── SBIS_Products.py        # Импорт ассортимента SBIS
│   └── Data_Workspace.py       # Коэффициенты и аналитика
├── db.py                       # SQLAlchemy engine/session, миграции Alembic
├── models.py                   # Модели Product и ProductImportLog
├── product_service.py          # Бизнес-логика каталога
├── product_repository.py       # Хранилище `product_items` (SQLite)
├── alembic/                    # Конфигурация и версии миграций
├── .streamlit/
│   ├── config.toml             # Конфигурация Streamlit
│   └── secrets.toml.example    # Шаблон секретов
├── requirements.txt            # Зависимости (Streamlit, SQLAlchemy, Alembic и др.)
└── ...                         # Клиенты, утилиты, мок-данные
```

## Перед началом

- Python 3.10+ (рекомендуется 3.11).
- Утилита `alembic` попадает через `requirements.txt`.
- По умолчанию используется SQLite-база `db.sqlite3` (создаётся автоматически).

## Установка и запуск локально

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Применяем миграции (создаст таблицы products и product_import_logs)
alembic upgrade head

# Запуск Streamlit-приложения (smoke-тест)
streamlit run streamlit_app.py
```

После запуска откроется главная страница с навигацией и подсказками по настройке окружения. Все страницы также доступны через встроенную навигацию (кнопки в шапке) или по прямым ссылкам.

## Конфигурация окружения

### Переменные окружения и `.env`

Приложение автоматически читает переменные из окружения и файла `.env` (через `python-dotenv`).

```bash
# .env
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/marketplace
```

- Если `DATABASE_URL` не задан, используется SQLite (`db.sqlite3`).
- Для PostgreSQL требуется драйвер `psycopg2-binary` (добавлен в `requirements.txt`).

### Секреты Streamlit

Создайте файл `.streamlit/secrets.toml`, используя шаблон:

По умолчанию интеграция с Wildberries использует домены `https://marketplace-api.wildberries.ru` и `https://content-api.wildberries.ru`. При необходимости их можно переопределить в секции `[wb]`.

```toml
# .streamlit/secrets.toml
DATABASE_URL = "postgresql+psycopg2://user:password@host:5432/marketplace"
OZON_CLIENT_ID = "your_client_id"
OZON_API_KEY = "your_api_key"

[wb]
WB_API_TOKEN = "your_wb_api_token"
WB_API_BASE = "https://marketplace-api.wildberries.ru"  # новое значение по умолчанию
WB_CONTENT_API_BASE = "https://content-api.wildberries.ru"  # отдельный домен для контент-методов
```

> Эти ключи необходимы для страниц Ozon и Wildberries. В облаке (Streamlit Cloud, Render, Railway) задайте их в настройках приложения.
> Настройка `[wb]` поддерживает как вложенные ключи, так и плоские `WB_API_TOKEN`/`WB_API_BASE` в целях обратной совместимости. Домен `https://suppliers-api.wildberries.ru` считается устаревшим — при его использовании приложение покажет предупреждение.

## Страницы приложения

| Страница | Назначение | Хранилище | Требуемые настройки |
|----------|------------|-----------|----------------------|
| `streamlit_app.py` | Онбординг, статусы конфигурации, быстрые ссылки | — | — |
| `pages/1_Products.py` | Каталог товаров с CRUD, импортом/экспортом, журналом | SQLAlchemy (`products`, `product_import_logs`) | `DATABASE_URL` (опционально) |
| `pages/2_OZON_Products.py` | Синхронизация товаров через Ozon Seller API | `product_items` (SQLite) | `OZON_CLIENT_ID`, `OZON_API_KEY` |
| `pages/WB_Products.py` | Просмотр и синхронизация ассортимента Wildberries (в том числе проверка подключения) | `product_items` (SQLite) | `WB_API_TOKEN` (опционально `WB_API_BASE`, `WB_CONTENT_API_BASE`) |
| `pages/SBIS_Products.py` | Импорт ассортимента SBIS из файлов | `product_items` (SQLite) | — |
| `pages/Data_Workspace.py` | Коэффициенты, аналитика и массовые правки | `product_items` (SQLite) | — |

## Поля каталога продуктов

| Поле модели | Заголовок шаблона | Описание |
|-------------|-------------------|----------|
| `sku` | `SKU` | Внутренний артикул карточки. |
| `seller_sku` | `Артикул продавца` | Артикул продавца из внешних систем. |
| `wb_sku` | `Артикул WB` | Идентификатор карточки на стороне Wildberries/маркетплейса. |
| `nm_id` | `NM ID` | Внутренний идентификатор (NM ID). |
| `title` | `Название` | Название товара (обязательное поле). |
| `brand` | `Бренд` | Бренд. |
| `category` | `Категория` | Категория/подкатегория. |
| `price_src` | `Текущая цена` | Цена на витрине до скидок. |
| `seller_discount_pct` | `Скидка, %` | Скидка продавца (0–100). |
| `product_cost` | `Себик` | Себестоимость единицы. |
| `shipping_cost` | `Транспортировка` | Стоимость доставки до склада маркетплейса. |
| `logistics_back_cost` | `Логистика возврата` | Ожидаемые затраты на возвраты. |
| `warehouse_coeff` | `Коэфф. склада` | Дополнительный коэффициент/стоимость склада. |
| `stock_wb` | `Остатки WB` | Остаток на складах маркетплейса. |
| `stock_seller` | `Остатки продавца` | Остаток на собственном складе. |
| `turnover_days` | `Оборачиваемость, дни` | Оборачиваемость в днях. |
| `weight_kg` | `Вес с упаковкой (кг)` | Вес единицы с упаковкой. |
| `package_l_cm` | `Длина упаковки, см` | Габарит упаковки по длине. |
| `package_w_cm` | `Ширина упаковки, см` | Габарит упаковки по ширине. |
| `package_h_cm` | `Высота упаковки, см` | Габарит упаковки по высоте. |
| `volume_l` | `Литраж` | Объём упаковки. Если не заполнено — вычисляется из габаритов. |
| `barcode` | `Штрихкод` | Штрихкод товара. |
| `comments` | `Комменты` | Свободный комментарий. |
| `is_active` | `Активен` | Флаг активной карточки. |
| `custom_data` | `Доп данные JSON` | Произвольные данные в формате JSON. Используйте ключи `commission_pct` и `tax_pct` для ставок комиссий и налогов. |

> Колонки `Итоговая цена (расчёт)`, `Цена со скидкой (расчёт)`, `Остаток общий (расчёт)`, `Комиссия`, `Налог`, `Маржа`, `Маржа, %` вычисляются автоматически и доступны только для чтения. Чтобы расчёты были корректны, заполните цену, скидку, себестоимость и логистику, а также ставки комиссий/налогов в `custom_data`.
>
> Импорт CSV/XLSX распознаёт русские заголовки из таблицы выше (включая варианты «литраж/Литраж», «транспортировка», «комменты» и т.д.) и автоматически сопоставляет их полям каталога. Обновлённый шаблон доступен в `data/products_import_sample.xlsx` и на вкладке «Импорт» интерфейса.

## Миграции и база данных

- Модуль `db.py` подхватывает `DATABASE_URL`, создаёт `engine` и `SessionLocal`. Для SQLite автоматически создаётся файл `db.sqlite3` в корне репозитория.
- При старте страницы вызывают `init_db()`, который пытается запустить `alembic upgrade head` (см. папку `alembic/`). Если Alembic недоступен, выполняется безопасный `Base.metadata.create_all()`.
- Для ручного управления миграциями используйте стандартные команды Alembic:
  ```bash
  alembic revision -m "comment"
  alembic upgrade head
  alembic downgrade -1
  ```

## Запуск в облаке

1. Загрузите репозиторий на платформу (Streamlit Cloud, Render, Railway и т.п.).
2. В настройках задайте переменные окружения/секреты:
   - `DATABASE_URL` (если нужна внешняя БД).
   - `OZON_CLIENT_ID`, `OZON_API_KEY`, `WB_API_TOKEN` для интеграций.
3. Укажите команду запуска: `streamlit run streamlit_app.py --server.port $PORT --server.headless true` (параметры могут отличаться в зависимости от платформы).
4. Проверьте, что `requirements.txt` установлен в окружении.

## Тема и стили

- Пользовательская тёмная тема описана в `demowb/styles.css`. В начале файла заданы переменные палитры (фон, акцент, текст). Изменяйте их, чтобы настроить оттенки приложения под свои нужды.
- Для подключения стилей добавлен модуль `demowb/ui.py` с функцией `inject_css(path)` и каркасом `card()` для визуально консистентных блоков. Все страницы вызывают `initialize_page`, который по умолчанию применяет тему; главная страница дополнительно подключает стили явно.
- Цветовая схема в `.streamlit/config.toml` синхронизирована с CSS (`primaryColor`, `backgroundColor`, `secondaryBackgroundColor`, `textColor`). Здесь же можно поменять базовые цвета и параметр `server.maxUploadSize` (по умолчанию 500 МБ).
- Чтобы временно отключить пользовательскую тему, передайте `inject_theme=False` при вызове `initialize_page(...)` на нужной странице и закомментируйте вызов `inject_css(...)`.

## Полезные утилиты

- `sync.py` — загрузка демонстрационных товаров (WB mock) в каталог SQLAlchemy.
- `sync_wb.py`, `sync_ozon.py` — адаптеры для синхронизации с внешними API.
- `data/` — образцы файлов для импорта (CSV/XLSX).

## Проверка работоспособности (Smoke-test)

```bash
streamlit run streamlit_app.py
```

Команда поднимет приложение локально и автоматически инициализирует базу. На главной странице отображаются статусы по конфигурации и быстрые ссылки на остальные страницы.

---
Если возникнут вопросы по конфигурации или расширению функциональности, начните с файла `streamlit_app.py`: он содержит подсказки по обязательным переменным и структуре проекта.

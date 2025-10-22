# demoWB — управление ассортиментом маркетплейсов

## Краткое резюме
- Репозиторий содержит набор Streamlit-страниц для работы с ассортиментом Wildberries, Ozon и SBIS, а также инструменты для управления ценами и анализа.
- Страница `pages/1_Products.py` превращена в полноценный каталог товаров: поддерживает редактирование через `st.data_editor`, управление пользовательскими полями (`custom_fields`), импорт Excel/CSV с журналом операций и экспорт с фильтрами.
- База данных для каталога переведена на SQLAlchemy-модель `Product` с хранением пользовательских колонок в JSON (`custom_fields`). Добавлена модель `ProductImportLog` для учёта импортов.
- Миграции Alembic настроены (директория `alembic/`), начальная миграция создаёт таблицы `products` и `product_import_logs`.
- Сохранены существующие сценарии работы с SQLite (`product_items`) для страниц WB/Ozon/SBIS и рабочее место коэффициентов.
- В папке `data/` появились шаблоны для импорта (`products_import_sample.csv`/`.xlsx`), которыми можно пользоваться без внешних API.

## Структура репозитория
```
/home/engine/project
├── alembic/                 # Конфигурация и версии миграций
│   ├── env.py
│   ├── script.py.mako
│   └── versions/20241022_0001_create_products_tables.py
├── alembic.ini              # Конфигурация Alembic
├── pages/
│   ├── 1_Products.py        # Управление каталогом с import/export и кастомными полями
│   ├── 2_OZON_Products.py   # Просмотр/синхронизация товаров Ozon
│   ├── WB_Products.py       # Просмотр/синхронизация Wildberries
│   ├── SBIS_Products.py     # Импорт ассортимента SBIS из Excel/CSV
│   └── Data_Workspace.py    # Работа с коэффициентами и аналитикой
├── product_service.py       # Сервисный слой для работы с Product/ProductImportLog
├── models.py                # SQLAlchemy-модели Product и ProductImportLog
├── db.py                    # Инициализация SQLAlchemy (engine, SessionLocal, Base)
├── sync.py                  # Загрузка демонстрационных товаров (WB mock)
├── data/
│   ├── sample_products.json
│   ├── products_import_sample.csv
│   └── products_import_sample.xlsx
├── requirements.txt
└── ... (остальные клиентские и утилитарные скрипты)
```

## Streamlit-страницы
| Файл | Назначение | Источник данных | Требуемые настройки |
|------|------------|-----------------|---------------------|
| `pages/1_Products.py` | Каталог товаров с CRUD, custom fields, импортом/экспортом и журналом импортов. Поддерживает шаблоны, массовые правки и центральный `st.data_editor`. | SQLAlchemy (`Product`, таблица `products`) | `DATABASE_URL` (опционально; по умолчанию `sqlite:///data.db`). |
| `pages/2_OZON_Products.py` | Просмотр и синхронизация ассортимента через Ozon Seller API. | SQLite (`sqlite.db`, таблица `product_items`) | `OZON_CLIENT_ID`, `OZON_API_KEY` в `st.secrets`. |
| `pages/WB_Products.py` | Синхронизация карточек Wildberries, фильтры и визуализация. | SQLite (`sqlite.db`, `product_items`) | `WB_API_TOKEN` в `st.secrets`. |
| `pages/SBIS_Products.py` | Импорт ассортимента SBIS из Excel/CSV, dry-run и загрузка в `product_items`. | SQLite (`sqlite.db`) | Настройка секретов не требуется. |
| `pages/Data_Workspace.py` | Управление коэффициентами (`coefficients`) и предпросмотр данных, экспорт/импорт CSV/XLSX. | SQLite (`sqlite.db`) | Настройка секретов не требуется. |

> Отдельного `app.py` нет: страницы запускаются командой `streamlit run pages/<имя>.py`.

## База данных и миграции
- **Основной каталог** использует SQLAlchemy:
  - `Product`: SKU, NM ID, базовые атрибуты, флаг активности, список изображений и JSON-поле `custom_fields` (через `MutableDict`).
  - `ProductImportLog`: информация о каждом импортировании (файл, статус, статистика, ошибки, детали).
- **Alembic**: конфигурация `alembic.ini` + стартовая миграция `20241022_0001_create_products_tables.py`. Для применения схемы выполните `alembic upgrade head` (см. инструкцию ниже).
- **Наследованный SQLite-слой** (`product_items`) по-прежнему используется страницами Ozon/WB/SBIS.

## Импорт и экспорт товаров (страница `1_Products.py`)
- **Редактирование**: `st.data_editor` позволяет добавлять строки, менять значения, скрывать/отображать кастомные колонки, выполнять массовые правки.
- **Пользовательские колонки**: данные сохраняются в `Product.custom_fields`. Можно добавлять новые поля из интерфейса и скрывать/показывать их в таблице.
- **Импорт**: поддержка CSV/XLSX (через pandas). Доступны шаблоны, выбор ключевого столбца (`sku` или `nm_id`), маппинг системных колонок, настройка custom_fields и валидация. Результат фиксируется в журнале импортов.
- **Экспорт**: фильтрация по поиску/бренду/статусу, выгрузка в CSV или Excel с выбранными custom_fields.Предпросмотр первых строк сопровождает кнопки скачивания.
- **Журнал**: вкладка «Журнал импортов» показывает последние операции и позволяет скачать CSV-лог.
- **Тестовые данные**: готовые файлы `data/products_import_sample.csv` и `.xlsx`, а также кнопка «Загрузить тестовые данные (WB mock)», которая наполняет таблицу демо-карточками.

## Настройка и запуск
1. **Создайте окружение и установите зависимости**
   ```bash
   cd /home/engine/project
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
2. **Примените миграции** (создание таблиц `products` и `product_import_logs`):
   ```bash
   alembic upgrade head
   ```
   По умолчанию будет использована база `sqlite:///data.db`. Переопределить URL можно переменной `DATABASE_URL` или значением в `st.secrets`.
3. **(Опционально) задайте секреты Streamlit** в `.streamlit/secrets.toml`, если нужна синхронизация с API маркетплейсов.
4. **Запускайте нужную страницу**:
   ```bash
   streamlit run pages/1_Products.py        # Каталог с import/export
   streamlit run pages/2_OZON_Products.py   # Ozon Seller API
   streamlit run pages/WB_Products.py       # Wildberries API
   streamlit run pages/SBIS_Products.py     # Импорт SBIS
   streamlit run pages/Data_Workspace.py    # Коэффициенты и аналитика
   ```
   Используйте параметры `--server.headless true --server.port <port>`, если требуется.

## Текущие ограничения и дальнейшие шаги
1. Страницы Ozon/WB/SBIS продолжают использовать отдельное хранилище `product_items` (SQLite); требуется унификация с SQLAlchemy-моделью `Product`.
2. Зависимости в `requirements.txt` не зафиксированы по версиям; рекомендуется добавить pinning и автоматические проверки.
3. Не настроены тесты/CI и автоматизированная проверка данных при импорте.
4. Отсутствует общая точка входа (`Home.py`) и навигация между страницами.
5. В репозитории остаются старые утилиты (`alex.py`, `v0.py`, `v3.py`, `V4.py`), которые можно вынести или архивировать.

---
Документ отражает текущее состояние проекта и служит руководством по использованию обновлённого каталога товаров и инструментов импорта/экспорта.

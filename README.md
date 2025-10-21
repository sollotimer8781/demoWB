# demoWB — проектный аудит и руководство по запуску

## Краткое резюме
- Репозиторий содержит несколько Streamlit-страниц для работы с ассортиментом маркетплейсов (Wildberries, Ozon, SBIS) и экспериментальные утилиты по обработке Excel-файлов.
- Основное хранилище данных — SQLite (`sqlite.db`) с таблицей `product_items`; параллельно присутствует заготовка под SQLAlchemy-модель `Product`, но без миграций и с ошибкой инициализации `engine` в `db.py`.
- Для интеграций с внешними API требуется настроить секреты (`WB_API_TOKEN`, `OZON_CLIENT_ID`, `OZON_API_KEY`). Реальная интеграция с SBIS ещё не реализована — доступен только импорт из файлов.
- Запуск приложений возможен постранично (`streamlit run pages/<page>.py`), центрального `app.py`/`Home.py` в репозитории нет.
- Зависимости перечислены в `requirements.txt` без фиксации версий; фактически во встроенном виртуальном окружении используются `streamlit 1.50.0`, `pandas 2.3.3`, `openpyxl 3.1.5`, `xlsxwriter 3.2.9`, `httpx 0.28.1`, `SQLAlchemy 2.0.44`.

## Структура репозитория
```
/home/engine/project
├── pages/                    # Активные Streamlit-страницы
│   ├── 1_Products.py         # Мок-страница каталога продукции
│   ├── 2_OZON_Products.py    # Просмотр/синхронизация товаров Ozon
│   ├── WB_Products.py        # Просмотр/синхронизация товаров Wildberries
│   ├── SBIS_Products.py      # Импорт ассортимента SBIS из Excel/CSV
│   └── Data_Workspace.py     # Рабочее место для коэффициентов и аналитики
├── sync.py                   # Синхронизация мок-данных (WB mock)
├── sync_wb.py                # Синхронизация Wildberries в SQLite
├── sync_ozon.py              # Синхронизация Ozon в SQLite
├── wb_client.py              # Клиент WB API (httpx)
├── ozon_client.py            # Клиент Ozon Seller API (httpx)
├── product_repository.py     # Работа с SQLite (таблица product_items)
├── data_workspace_repository.py  # Кэфы/правила ценообразования в SQLite
├── db.py / models.py         # Черновая обёртка SQLAlchemy (таблица products)
├── data/sample_products.json # Исходник мок-товаров для WB
├── sqlite.db                 # Фактическое хранилище продуктов
├── *.py (alex.py, v0.py, v3.py, V4.py)  # Наследие: утилиты по Excel-каталогу
└── тест данные.xlsx          # Пример входного Excel-файла
```

## Streamlit-страницы
| Файл | Назначение | Источник данных | Требуемые секреты/настройки |
|------|------------|-----------------|------------------------------|
| `pages/1_Products.py` | Мок-каталог товаров (Wildberries) с поиском и карточками. При первом запуске загружает данные из `data/sample_products.json` (через `sync.py`). | SQLite (`data.db` по умолчанию через SQLAlchemy) | `DATABASE_URL` (опционально); отсутствие `.streamlit/secrets.toml` приводит к исключению при импорте `db.py` вне Streamlit. |
| `pages/2_OZON_Products.py` | Просмотр и синхронизация ассортимента через Ozon Seller API; фильтры и детальный просмотр. | SQLite (`sqlite.db`, таблица `product_items`) | `OZON_CLIENT_ID`, `OZON_API_KEY` в `st.secrets`. |
| `pages/WB_Products.py` | Поддержка синхронизации карточек Wildberries (cursor API v1/v2), визуализация и фильтры. | SQLite (`sqlite.db`, `product_items`) | `WB_API_TOKEN` в `st.secrets`. |
| `pages/SBIS_Products.py` | Импорт ассортимента SBIS из Excel/CSV, dry-run, загрузка в `product_items`, просмотр и фильтры. | SQLite (`sqlite.db`) | Секреты не требуются; ожидаются шаблоны в `data/sbis`, но они отсутствуют. |
| `pages/Data_Workspace.py` | Управление коэффициентами (`coefficients`) и предпросмотр данных из `product_items`; экспорт/импорт CSV/XLSX, генерация коэффициентов. | SQLite (`sqlite.db`, таблицы `product_items`, `coefficients`, `pricing_rules`) | Секреты не требуются. |

> **Важно:** центрального файла `app.py`/`Home.py` нет, поэтому переключение между страницами через стандартное меню Streamlit недоступно. Каждая страница запускается отдельной командой.

## База данных и модели
- **SQLite** — основное хранилище (`/home/engine/project/sqlite.db`).
  - Таблица `product_items` создаётся в `product_repository.ensure_schema()` и используется Wildberries, Ozon и SBIS страницами.
  - Таблицы `coefficients` и `pricing_rules` управляются через `data_workspace_repository.py`.
- **SQLAlchemy** — файл `db.py` предполагает использование `DATABASE_URL` (по умолчанию `sqlite:///data.db`), но содержит опечатку `gine = _create_engine()` и выбрасывает `StreamlitSecretNotFoundError` при отсутствии `.streamlit/secrets.toml`. Таблица `products` объявлена в `models.py`, миграций Alembic нет.
- **Миграции** отсутствуют; структура БД поддерживается вручную через `ensure_schema`.

## Импорт/экспорт данных
- `pages/SBIS_Products.py` — полноценный мастер импорта Excel/CSV: выбор ключей, картирование колонок, dry-run (`_simulate_upsert`) и запись в SQLite. Экспорт отсутствует, но доступны детальные просмотры.
- `pages/Data_Workspace.py` — экспорт коэффициентов в CSV/XLSX и импорт с опцией полной замены (`replace_all_coefficients`).
- `alex.py`, `v0.py`, `v3.py`, `V4.py` — наследуемые утилиты для объединения Excel-файлов и поддержания `catalog.csv`; в текущий основной поток не интегрированы.

## Интеграции с внешними API
- **Wildberries** — `wb_client.py` реализует cursor API (v1/v2) с ретраями, нормализацией карточек и преобразованием медиа. Требуется `WB_API_TOKEN` в `st.secrets` или переменных окружения Streamlit.
- **Ozon Seller API** — `ozon_client.py` реализует методы `/v2/product/list` и `/v3/product/info/list`, объединяет карточки и сохраняет в SQLite. Требуются `OZON_CLIENT_ID` и `OZON_API_KEY`.
- **SBIS** — API-интеграция не реализована; вместо этого есть UI для импорта файлов и пометка в интерфейсе о будущем подключении.

## Зависимости
`requirements.txt` содержит непинованные пакеты:
```
streamlit
pandas
openpyxl
xlsxwriter
httpx
sqlalchemy
```
Фактические версии, установленные во встроенном виртуальном окружении `.venv`:
- Streamlit — **1.50.0**
- pandas — **2.3.3**
- openpyxl — **3.1.5**
- XlsxWriter — **3.2.9**
- httpx — **0.28.1**
- SQLAlchemy — **2.0.44**

Рекомендуется зафиксировать версии в `requirements.txt/poetry.lock`, чтобы исключить неожиданные регрессии.

## Настройка и запуск локально
1. **Предварительные требования**
   - Python 3.11+
   - Установленный `virtualenv` или `uv` (опционально)
   - Доступ к API-ключам (WB/Ozon) при необходимости синхронизации.
2. **Развёртывание окружения**
   ```bash
   cd /home/engine/project
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Конфигурация секретов Streamlit** (файл `.streamlit/secrets.toml`):
   ```toml
   [secrets]
   WB_API_TOKEN = "..."
   OZON_CLIENT_ID = "..."
   OZON_API_KEY = "..."
   # при необходимости подключить внешнюю БД
   DATABASE_URL = "postgresql+psycopg://user:pass@host:5432/dbname"
   ```
   При использовании только локального SQLite требования к секретам можно опустить, но часть страниц (WB/Ozon) будет работать только в режиме просмотра без синхронизации.
4. **Запуск страниц** (каждая отдельно):
   ```bash
   streamlit run pages/1_Products.py        # мок-данные
   streamlit run pages/WB_Products.py       # Wildberries
   streamlit run pages/2_OZON_Products.py   # Ozon Seller API
   streamlit run pages/SBIS_Products.py     # Импорт SBIS
   streamlit run pages/Data_Workspace.py    # Коэффициенты
   ```
   При необходимости используйте `--server.headless true --server.port <port>`.

### Результаты локального запуска
- Проверен запуск `streamlit run pages/1_Products.py` и `streamlit run pages/WB_Products.py` в headless-режиме — сервер стартует, страницы отображают предупреждения об отсутствующих секретах.
- Скриншоты не приложены из-за текстового окружения; при наличии токенов необходимо протестировать синхронизацию вручную.

## Выявленные пробелы и риски
1. Ошибка инициализации SQLAlchemy (`gine = _create_engine()`) и жёсткая зависимость `db.py` от наличия `st.secrets` мешают использованию моделей вне Streamlit.
2. Отсутствует единая точка входа Streamlit (`app.py`/`Home.py`), поэтому навигация между страницами неудобна.
3. Нет миграций Alembic и единого слоя доступа к данным — coexist двух подходов (SQLAlchemy + ручной SQLite).
4. `product_repository.py` хардкодит абсолютный путь `/home/engine/project/sqlite.db`, что усложняет деплой и тестирование.
5. Нет автоматических тестов, проверок линтеров и данных фикстур.
6. В `requirements.txt` отсутствуют конкретные версии; возможны несовместимости при развёртывании.
7. Шаблоны файлов для SBIS, упомянутые в UI, отсутствуют в `data/sbis`.
8. В репозитории остаются устаревшие скрипты (`alex.py`, `v0.py`, `v3.py`, `V4.py`) без документации и интеграции.

## Рекомендации и последующие задачи
1. **Починить модуль `db.py`**: исправить `engine = _create_engine()`, добавить безопасное поведение при отсутствии `st.secrets`, вынести конфигурацию в `.env`/`settings`.
2. **Добавить точку входа Streamlit** (`app.py` или `Home.py`) с меню по страницам и едиными настройками `st.set_page_config`.
3. **Унифицировать доступ к данным**: выбрать SQLAlchemy или чистый SQLite, вынести пути в конфигурацию, подготовить Alembic-миграции.
4. **Подготовить шаблоны и документацию** для импорта (папка `data/sbis`, описание ожидаемых колонок) и удалить/архивировать устаревшие прототипы Excel.
5. **Настроить CI/проверки качества**: линтеры, `pytest`, статический анализ, автоматический импортер зависимостей.
6. **Расширить интеграции**: реализовать SBIS API-клиент, добавить пагинацию и обработку ошибок для WB/Ozon, предусмотреть логирование.
7. **Зафиксировать версии зависимостей** и добавить инструкции по обновлению.
8. **Документировать переменные окружения** и сценарии деплоя (Docker/Compose при необходимости).

---
Настоящий README фиксирует текущее состояние проекта demoWB и служит отправной точкой для дальнейшего развития и упорядочивания кода.

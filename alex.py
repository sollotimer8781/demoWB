import io
import pandas as pd
import streamlit as st
import os

st.set_page_config(page_title="Merge Excel файлов", layout="wide")
def sync_catalog(df_update, catalog_file, key_cat, key_update):
    import os
    import pandas as pd

    # Загружаем каталог
    if os.path.exists(catalog_file):
        df_cat = pd.read_csv(catalog_file)
    else:
        st.error("Каталог не найден!")
        return

    # Добавляем новые столбцы, если есть
    for col in df_update.columns:
        if col not in df_cat.columns:
            df_cat[col] = None

    # Обновление данных по ключу
    updates = 0
    for _, new_row in df_update.iterrows():
        key_val = new_row.get(key_update)
        if key_val is None:
            continue
        match_idx = df_cat[df_cat[key_cat] == key_val].index
        if len(match_idx):
            idx = match_idx[0]
            for col in df_update.columns:
                if pd.notnull(new_row[col]) and col != key_update:
                    df_cat.at[idx, col] = new_row[col]
            updates += 1
        else:
            # Добавить новую строку с переносом данных из df_update
            empty_row = {col: None for col in df_cat.columns}
            for col in df_update.columns:
                empty_row[col] = new_row[col]
            df_cat = pd.concat([df_cat, pd.DataFrame([empty_row])], ignore_index=True)
            updates += 1

    df_cat.to_csv(catalog_file, index=False)
    return updates

def clean_catalog(df):
    seen = {}
    for col in df.columns:
        val = tuple(df[col].fillna("").astype(str))
        if val in seen:
            df = df.drop(columns=[col])
        else:
            seen[val] = col
    df = df.dropna(axis=1, how='all')
    df = df.drop_duplicates()
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.replace('\s+', ' ', regex=True)
    return df

def show_catalog_tab():
    import os
    import pandas as pd
    st.title("Каталог")
    catalog_file = "catalog.csv"
    if os.path.exists(catalog_file):
        df_cat = pd.read_csv(catalog_file)
    else:
        st.error("Файл каталога не найден!")
        return
    st.write("Текущий каталог:")
    st.dataframe(df_cat, use_container_width=True)

    # Загрузка и обновление каталога по ключу
    st.markdown("**Обновить/добавить товары из файла по выбранному ключу**")
    uploaded_file = st.file_uploader("Загрузить файл для обновления (CSV/XLSX)", type=["csv", "xlsx"])
    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df_update = pd.read_csv(uploaded_file)
        else:
            df_update = pd.read_excel(uploaded_file)
        st.write(f"Столбцы обновляющего файла: {list(df_update.columns)}")
        st.write(f"Столбцы каталога: {list(df_cat.columns)}")
        if st.checkbox("Показать данные для обновления"):
            st.dataframe(df_update)
        # Выбор ключевого поля для обновления
        key_cat = st.selectbox("Выберите ключевое поле в каталоге", list(df_cat.columns))
        key_update = st.selectbox("Выберите ключевое поле в файле обновления", list(df_update.columns))
        if st.button("Обновить каталог по этим ключам", use_container_width=True):
            count = sync_catalog(df_update, catalog_file, key_cat, key_update)
            st.success(f"Каталог обновлён ({count} строк изменено/добавлено).")

    # Кнопка очистки
    st.markdown("**Очистить и нормализовать базу**")
    if st.button("Провести автоматическую очистку каталога", use_container_width=True):
        with st.spinner("Идёт очистка..."):
            df_clean = clean_catalog(df_cat)
            df_clean.to_csv(catalog_file, index=False)
        st.success("✅ Каталог очищен и нормализован!")
        st.experimental_rerun()

    # Динамическая форма для ручного добавления/редактирования — на основе структуры каталога
    with st.form("add_item"):
        st.write("Добавить или обновить товар вручную")
        values = {}
        for col in df_cat.columns:
            values[col] = st.text_input(col)
        submitted = st.form_submit_button("Добавить / Обновить")
        if submitted:
            key_col = st.selectbox("Выберите ключевое поле для ручного добавления", list(df_cat.columns))
            key_val = values[key_col]
            idx = df_cat[df_cat[key_col] == key_val].index
            if len(idx):
                # обновление
                for col in df_cat.columns:
                    df_cat.at[idx[0], col] = values[col]
                st.success("Запись обновлена!")
            else:
                # добавление
                df_cat = pd.concat([df_cat, pd.DataFrame([values])], ignore_index=True)
                st.success("Запись добавлена!")
            df_cat.to_csv(catalog_file, index=False)
            st.experimental_rerun()

    st.download_button("Скачать каталог CSV", df_cat.to_csv(index=False).encode(), "catalog.csv")




tab1, tab2 = st.tabs(["Объединение файлов", "Каталог"])
with tab2:
    show_catalog_tab()

           
with tab1:
        st.title("Объединение Excel файлов по ключевому полю")
        st.caption("Загрузите 2 или 3 Excel файла, выберите ключ для объединения. Результат содержит только строки, найденные во ВСЕХ файлах.")


def sync_catalog(df, catalog_file="catalog.csv"):
    # df — это DataFrame исходных (новых) данных
    # catalog_file — путь к файлу каталога
    if os.path.exists(catalog_file):
        df_cat = pd.read_csv(catalog_file)
    else:
        # Начальный каталог, создаём новый
        df_cat = pd.DataFrame(columns=["Код ТН ВЭД", "Наименование", "Описание", "Ед. изм.", "Страна"])
    # Добавляем новые столбцы, если появились
    for col in df.columns:
        if col not in df_cat.columns:
            df_cat[col] = None
    # Проходим по исходным данным
    for _, row in df.iterrows():
        code = row["Код ТН ВЭД"]
        # Если товара нет — добавить
        if code not in df_cat["Код ТН ВЭД"].values:
            new_row = {col: row.get(col, None) for col in df_cat.columns}
            df_cat = df_cat.append(new_row, ignore_index=True)
        else:
            # Если есть — обновить новые значения (только не пустые)
            idx = df_cat[df_cat["Код ТН ВЭД"] == code].index[0]
            for col in df.columns:
                if pd.notnull(row[col]):
                    df_cat.at[idx, col] = row[col]
    # Сохраняем
    df_cat.to_csv(catalog_file, index=False)

# В конце обработки (на вкладке объединения)
# sync_catalog(df)   # df — твоя итоговая, обработанная таблица
    
# ---------- Sidebar ----------
with st.sidebar:
    st.header("Параметры чтения")
    sheet_name = st.text_input("Имя листа (пусто = первый)", value="")
    header_row = st.number_input("Строка заголовков (0-индекс)", min_value=0, value=0, step=1)
    normalize_key = st.checkbox("Нормализовать ключ (trim/lower/без неразрывных)", value=True)
    output_name = st.text_input("Имя итогового Excel", value="merged_output.xlsx")
    
    st.divider()
    st.subheader("Тип объединения")
    join_type = st.radio(
        "Выберите стратегию",
        options=["inner", "outer"],
        index=0,
        help="inner = только совпадающие ключи во всех файлах (без пустых строк)\nouter = все ключи, даже если есть только в одном файле (могут быть пустые значения)"
    )

def read_xlsx(file, sheet_name=None, header_row=0):
    """Безопасное чтение Excel с обработкой ошибок"""
    if file is None:
        return None
    try:
        if sheet_name and str(sheet_name).strip():
            return pd.read_excel(file, sheet_name=str(sheet_name).strip(), header=header_row)
        return pd.read_excel(file, header=header_row)
    except Exception as e:
        st.error(f"Ошибка чтения {getattr(file, 'name', 'файла')}: {e}")
        return None

def normalize_series_key(s: pd.Series) -> pd.Series:
    """Нормализация ключа: trim, lower, убрать неразрывные пробелы"""
    return (s.astype(str)
            .str.replace("\u00A0", "", regex=False)
            .str.strip()
            .str.lower())

# ---------- Загрузка файлов ----------
st.subheader("Шаг 1: Загрузите файлы (минимум 2)")
col1, col2, col3 = st.columns(3)
with col1:
    f1 = st.file_uploader("Excel файл #1", type=["xlsx", "xls"], key="file1")
with col2:
    f2 = st.file_uploader("Excel файл #2", type=["xlsx", "xls"], key="file2")
with col3:
    f3 = st.file_uploader("Excel файл #3 (опционально)", type=["xlsx", "xls"], key="file3")

# ---------- Чтение файлов ----------
df1 = read_xlsx(f1, sheet_name, header_row)
df2 = read_xlsx(f2, sheet_name, header_row)
df3 = read_xlsx(f3, sheet_name, header_row)

# ---------- Собираем только загруженные файлы ----------
loaded_dfs = []
loaded_names = []
all_dfs = [df1, df2, df3]
all_names = ["Файл_1", "Файл_2", "Файл_3"]

for df, name in zip(all_dfs, all_names):
    if df is not None:
        loaded_dfs.append(df)
        loaded_names.append(name)

# ---------- Показываем превью ----------
if loaded_dfs:
    st.info(f"Загружено файлов: {len(loaded_dfs)}")
    preview_tabs = st.tabs(loaded_names)
    for tab, df, name in zip(preview_tabs, loaded_dfs, loaded_names):
        with tab:
            st.write(f"**{name}**: {df.shape[0]} строк × {df.shape[1]} колонок")
            st.dataframe(df.head(50), use_container_width=True)

# ---------- ПРОВЕРКА: минимум 2 файла ----------
if len(loaded_dfs) < 2:
    st.warning("⚠️ Загрузите минимум два файла для объединения")
else:
    # ---------- Работаем с загруженными файлами ----------
    st.divider()
    st.success(f"✅ Загружено {len(loaded_dfs)} файлов. Готово к объединению!")

    # ---------- Шаг 2: Выбор ключа ----------
    st.subheader("Шаг 2: Настройка ключевого поля")
    st.write("Выберите колонку-ключ в каждом файле.")

    result_key_name = st.text_input("Имя ключа в результате", value="Ключ")

    # Динамически создаём selectbox для каждого загруженного файла
    key_cols = []
    key_select_cols = st.columns(len(loaded_dfs))
    for idx, (col, df, name) in enumerate(zip(key_select_cols, loaded_dfs, loaded_names)):
        with col:
            key = st.selectbox(f"Ключ в {name}", options=list(df.columns), index=0, key=f"key_{idx}")
            key_cols.append(key)

    st.divider()

    # ---------- Шаг 3: Дополнительные поля ----------
    st.subheader("Шаг 3: Выбор дополнительных полей")
    st.write("Выберите поля из каждого файла (кроме ключа). Они получат префиксы [1]/[2]/[3].")

    add_cols = []
    field_tabs = st.tabs([f"Поля {name}" for name in loaded_names])
    
    for idx, (tab, df, name, key_col) in enumerate(zip(field_tabs, loaded_dfs, loaded_names, key_cols)):
        with tab:
            available_cols = [c for c in df.columns if c != key_col]
            selected = st.multiselect(
                f"Выберите поля из {name}",
                options=available_cols,
                default=[],
                key=f"fields_{idx}"
            )
            add_cols.append(selected)

    st.divider()

    # ---------- Функция объединения ----------
    def build_merged_result(dfs_list, key_cols_list, add_cols_list, names_list, result_key_name, join_type, do_normalize):
        """Объединяет 2 или 3 DataFrame по ключу с префиксами для полей"""
        
        # Подготовка каждого DataFrame
        prepared_dfs = []
        for idx, (df, key_col, extra_cols, name) in enumerate(zip(dfs_list, key_cols_list, add_cols_list, names_list)):
            # Проверка ключа
            if key_col not in df.columns:
                raise ValueError(f"В {name} нет столбца '{key_col}'")
            
            # Выбор нужных колонок
            cols = [key_col] + extra_cols
            temp_df = df[cols].copy()
            
            # Нормализация ключа
            if do_normalize:
                temp_df[key_col] = normalize_series_key(temp_df[key_col])
            
            # Переименование ключа
            temp_df = temp_df.rename(columns={key_col: result_key_name})
            
            # Префиксы для не-ключевых полей
            prefix = f"[{idx+1}] "
            temp_df = temp_df.rename(columns={
                col: f"{prefix}{col}" 
                for col in temp_df.columns 
                if col != result_key_name
            })
            
            prepared_dfs.append(temp_df)
        
        # Последовательное объединение
        result = prepared_dfs[0]
        for next_df in prepared_dfs[1:]:
            result = pd.merge(result, next_df, on=result_key_name, how=join_type)
        
        # КРИТИЧНО: если использовали inner, пустых строк не будет
        # Если outer - удалим строки, где ВСЕ не-ключевые поля пустые
        if join_type == "outer":
            # Находим колонки кроме ключа
            non_key_cols = [c for c in result.columns if c != result_key_name]
            # Удаляем строки, где ВСЕ значения (кроме ключа) NaN
            result = result.dropna(subset=non_key_cols, how='all')
        
        return result
def sync_catalog(df, catalog_file="catalog.csv"):
    # df — это DataFrame исходных (новых) данных
    # catalog_file — путь к файлу каталога
    if os.path.exists(catalog_file):
        df_cat = pd.read_csv(catalog_file)
    else:
        # Начальный каталог, создаём новый
        df_cat = pd.DataFrame(columns=["Код ТН ВЭД", "Наименование", "Описание", "Ед. изм.", "Страна"])
    # Добавляем новые столбцы, если появились
    for col in df.columns:
        if col not in df_cat.columns:
            df_cat[col] = None
    # Проходим по исходным данным
    for _, row in df.iterrows():
        code = row["Код ТН ВЭД"]
        # Если товара нет — добавить
        if code not in df_cat["Код ТН ВЭД"].values:
            new_row = {col: row.get(col, None) for col in df_cat.columns}
            df_cat = df_cat.append(new_row, ignore_index=True)
        else:
            # Если есть — обновить новые значения (только не пустые)
            idx = df_cat[df_cat["Код ТН ВЭД"] == code].index[0]
            for col in df.columns:
                if pd.notnull(row[col]):
                    df_cat.at[idx, col] = row[col]
    # Сохраняем
    df_cat.to_csv(catalog_file, index=False)

# В конце обработки (на вкладке объединения)
# sync_catalog(df)   # df — твоя итоговая, обработанная таблица

    # ---------- Шаг 4: Объединение и выгрузка ----------
    st.subheader("Шаг 4: Создание результата")

    if st.button("🚀 Объединить и создать Excel", use_container_width=True, type="primary"):
        try:
            with st.spinner("Объединяем данные..."):
                result = build_merged_result(
                    dfs_list=loaded_dfs,
                    key_cols_list=key_cols,
                    add_cols_list=add_cols,
                    names_list=loaded_names,
                    result_key_name=result_key_name,
                    join_type=join_type,
                    do_normalize=normalize_key
                )
            
            st.success(f"✅ Объединено {len(result)} строк (пустые строки удалены)")
            
            # Статистика по пропущенным
            if join_type == "inner":
                st.info("Используется INNER JOIN: в результат попали только ключи, найденные во всех файлах одновременно.")
            
            # Превью результата
            with st.expander("👁️ Предпросмотр результата (первые 100 строк)"):
                st.dataframe(result.head(100), use_container_width=True)
            
            # Создание Excel
            with st.spinner("Формируем Excel файл..."):
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                    # Исходные файлы
                    for df, name in zip(loaded_dfs, loaded_names):
                        df.to_excel(writer, sheet_name=name, index=False)
                    
                    # Результат
                    result.to_excel(writer, sheet_name="Merge", index=False)
                    
                    # Автоширина колонок
                    for sheet_name, worksheet in writer.sheets.items():
                        if sheet_name == "Merge":
                            max_col = result.shape[1]
                        else:
                            idx = loaded_names.index(sheet_name)
                            max_col = loaded_dfs[idx].shape[1]
                        worksheet.set_column(0, max(0, max_col - 1), 18)
                
                bio.seek(0)
            
            # Кнопка скачивания
            st.download_button(
                label="📥 Скачать Excel файл",
                data=bio,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"❌ Ошибка при объединении: {e}")
            st.exception(e)
    st.success(f"✅ Объединено {len(result)} строк (пустые строки удалены)")

# Кнопка синхронизации каталога
if st.button("🔄 Синхронизировать каталог с новыми данными", use_container_width=True):
    with st.spinner("Обновляем каталог..."):
        sync_catalog(result)
    st.success("✅ Каталог успешно обновлён! Проверьте вкладку 'Каталог'.")










            

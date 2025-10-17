import io
import pandas as pd
import streamlit as st
import os

tab1, tab2 = st.tabs(["Объединение файлов", "Каталог"])
def sync_catalog(df_update, catalog_file, key_cat, key_update):
    import pandas as pd
    import os
    if os.path.exists(catalog_file):
        df_cat = pd.read_csv(catalog_file)
    else:
        st.warning("Каталог не найден! Создайте его сначала.")
        return
    # Добавить новые столбцы, если появились
    for col in df_update.columns:
        if col not in df_cat.columns:
            df_cat[col] = None
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
            new_entry = {col: None for col in df_cat.columns}
            for col in df_update.columns:
                new_entry[col] = new_row.get(col, None)
            df_cat = pd.concat([df_cat, pd.DataFrame([new_entry])], ignore_index=True)
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
    import pandas as pd
    import os
    st.title("Каталог")
    catalog_file = "catalog.csv"
    # Пустой каталог при первом запуске
    if not os.path.exists(catalog_file):
        st.info("Каталог ещё не создан! Загрузите первый файл или укажите поля вручную.")
        # Загрузка первого файла для каталога
        uploaded_file = st.file_uploader("Создать каталог из файла (CSV/XLSX)", type=["csv", "xlsx"])
        if uploaded_file:
            if uploaded_file.name.endswith('.csv'):
                df_cat = pd.read_csv(uploaded_file)
            else:
                df_cat = pd.read_excel(uploaded_file)
            df_cat.to_csv(catalog_file, index=False)
            st.success("Каталог сохранён! Перезагрузите страницу.")
            st.stop()
        # Ручное создание структуры
        with st.form("init_catalog"):
            raw_columns = st.text_input("Названия столбцов через запятую (например: Артикул,Наименование,Цена)")
            submitted = st.form_submit_button("Создать пустой каталог")
            if submitted and raw_columns:
                cols = [c.strip() for c in raw_columns.split(",")]
                df_cat = pd.DataFrame(columns=cols)
                df_cat.to_csv(catalog_file, index=False)
                st.success(f"Пустой каталог с поляcми {cols} создан! Перезагрузите.")
                st.stop()
        return

    df_cat = pd.read_csv(catalog_file)
    st.write("Текущий каталог:")
    st.dataframe(df_cat, use_container_width=True)

    # ОБНОВЛЕНИЕ каталога по одному файлу с ручным выбором ключа
    st.markdown("**Обновить/добавить товары из файла по ключу**")
    uploaded_file = st.file_uploader("Загрузить файл для обновления/добавления (CSV/XLSX)", type=["csv", "xlsx"], key="updatefile")
    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df_update = pd.read_csv(uploaded_file)
        else:
            df_update = pd.read_excel(uploaded_file)
        st.write("Столбцы в загружаемом файле:", list(df_update.columns))
        st.write("Столбцы в каталоге:", list(df_cat.columns))
        key_cat = st.selectbox("Ключ для поиска в каталоге", options=list(df_cat.columns), key="keycat")
        key_update = st.selectbox("Ключ для поиска в обновлении", options=list(df_update.columns), key="keyupdate")
        if st.button("Синхронизировать каталогу с этим файлом"):
            updates = sync_catalog(df_update, catalog_file, key_cat, key_update)
            st.success(f"Обновлено/добавлено записей: {updates}")
            st.experimental_rerun()

    # Кнопка очистки
    st.markdown("**Очистить и нормализовать каталог**")
    if st.button("Провести автоматическую очистку", use_container_width=True):
        with st.spinner("Очистка..."):
            df_clean = clean_catalog(df_cat)
            df_clean.to_csv(catalog_file, index=False)
        st.success("Каталог очищен! Обновите страницу.")
        st.experimental_rerun()

    # Динамическая форма для добавления/редактирования — поля берутся из структуры каталога
    with st.form("addmanual"):
        st.write("Добавить/обновить товар вручную")
        record = {}
        for col in df_cat.columns:
            record[col] = st.text_input(col, key=f"manual_{col}")
        submitted = st.form_submit_button("Добавить / Обновить")
        if submitted:
            key_col = st.selectbox("Ключевое поле для добавления/редактирования", options=list(df_cat.columns), key="manual_key_field")
            key_val = record[key_col]
            idx = df_cat[df_cat[key_col] == key_val].index
            if len(idx):
                for col in df_cat.columns:
                    df_cat.at[idx[0], col] = record[col]
                st.success("Запись обновлена!")
            else:
                df_cat = pd.concat([df_cat, pd.DataFrame([record])], ignore_index=True)
                st.success("Запись добавлена!")
            df_cat.to_csv(catalog_file, index=False)
            st.experimental_rerun()

    st.download_button("Скачать каталог CSV", df_cat.to_csv(index=False).encode(), "catalog.csv")

# И наконец сам блок tab2 — только вызов функции:
with tab2:
    show_catalog_tab()
with tab1:
    st.title("Объединение файлов (аналитика)")
    st.caption("Загрузите 2 или 3 файла (Excel/CSV), выберите ключевое поле для объединения, получите финальный результат.")

    # Шаг 1. Загрузка файлов
    uploaded_files = st.file_uploader(
        "Выберите два или три файла (.xlsx, .csv)", 
        type=["xlsx", "csv"], 
        accept_multiple_files=True
    )

    # Шаг 2. Предпросмотр и анализ файлов
    dfs = []
    file_names = []
    if uploaded_files and len(uploaded_files) >= 2:
        for i, f in enumerate(uploaded_files):
            if f.name.endswith('.csv'):
                df = pd.read_csv(f)
            else:
                df = pd.read_excel(f)
            dfs.append(df)
            file_names.append(f.name)
            st.write(f"**{f.name}:** строк: {df.shape[0]}, столбцов: {df.shape[1]}")
            st.dataframe(df.head(7))
        # Найти все возможные ключи для объединения
        all_columns = sorted(set(col for df in dfs for col in df.columns))
        key = st.selectbox("Выберите ключевое поле для объединения", all_columns)
        
        # Выбор типа объединения (inner/outer)
        join_type = st.radio("Тип объединения:", ["inner", "outer"], index=0, help="inner - только совпадающие ключи, outer - все ключи (может быть больше пустых строк)")

        if st.button("Объединить файлы"):
            # Слияние всех файлов по выбранному ключу
            result = dfs[0]
            for idx, df_next in enumerate(dfs[1:], start=2):
                result = pd.merge(result, df_next, on=key, how=join_type, suffixes=('', f'_f{idx}'))
            
            # Сводная статистика
            st.success(f"✅ Получен результат: {result.shape[0]} строк, {result.shape[1]} столбцов")
            st.dataframe(result)
            st.markdown("**Статистика по исходным файлам:**")
            for name, dfprev in zip(file_names, dfs):
                uniq_keys = dfprev[key].nunique()
                st.write(f"{name}: уникальных ключей: {uniq_keys}")

            # Кнопка скачивания итогового результата
            st.download_button(
                "Скачать объединённый результат CSV",
                result.to_csv(index=False).encode(),
                "merged_result.csv"
            )

            # Кнопка выгрузки в Excel (опционально)
            import io
            output_xlsx = io.BytesIO()
            with pd.ExcelWriter(output_xlsx, engine="xlsxwriter") as writer:
                result.to_excel(writer, index=False)
            st.download_button("Скачать результат в Excel", data=output_xlsx.getvalue(), file_name="merged_result.xlsx")

    else:
        st.info("Для объединения загрузите минимум два файла.")

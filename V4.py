import streamlit as st
import pandas as pd
import sqlite3
import io
import os

DB_PATH = "catalog.db"

# ======================
# --- БАЗА ДАННЫХ ---
# ======================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        code TEXT,
        country TEXT,
        description TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

def add_product(values):
    conn = sqlite3.connect(DB_PATH)
    keys = ','.join(values.keys())
    qmarks = ','.join('?' * len(values))
    conn.execute(f"INSERT INTO products ({keys}) VALUES ({qmarks})", tuple(values.values()))
    conn.commit()
    conn.close()

def update_product(product_id, values):
    conn = sqlite3.connect(DB_PATH)
    sets = ','.join([f"{k}=?" for k in values.keys()])
    conn.execute(f"UPDATE products SET {sets}, updated_at=CURRENT_TIMESTAMP WHERE id=?", (*values.values(), product_id))
    conn.commit()
    conn.close()

def load_all_products():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

def delete_product(product_id):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM products WHERE id=?", (product_id,))
    conn.commit()
    conn.close()

init_db()

# ======================
tab1, tab2 = st.tabs(["Объединение файлов", "Каталог"])

# ======================
# --- TAB1: ОБЪЕДИНЕНИЕ --
# ======================
with tab1:
    with st.sidebar:
        st.header("Параметры чтения")
        sheet_name = st.text_input("Имя листа (пусто = первый)", value="")
        header_row = st.number_input("Строка заголовков (0-индекс)", min_value=0, value=0, step=1)
        normalize_key = st.checkbox("Нормализовать ключ (trim/lower/без неразрывных)", value=True)
        output_name = st.text_input("Имя итогового Excel", value="merged_output.xlsx")
        st.divider()
        st.subheader("Тип объединения")
        join_type = st.radio("Выберите стратегию", options=["inner", "outer"], index=0,
                             help="inner = только совпадающие ключи, outer = все ключи (могут быть пустые поля)")

    def read_xlsx(file, sheet_name=None, header_row=0):
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
        return (s.astype(str)
                  .str.replace("\u00A0", "", regex=False)
                  .str.strip()
                  .str.lower())

    st.subheader("Шаг 1: Загрузите файлы (минимум 2)")
    col1, col2, col3 = st.columns(3)
    with col1:
        f1 = st.file_uploader("Excel файл #1", type=["xlsx", "xls"], key="file1")
    with col2:
        f2 = st.file_uploader("Excel файл #2", type=["xlsx", "xls"], key="file2")
    with col3:
        f3 = st.file_uploader("Excel файл #3 (опционально)", type=["xlsx", "xls"], key="file3")

    df1 = read_xlsx(f1, sheet_name, header_row)
    df2 = read_xlsx(f2, sheet_name, header_row)
    df3 = read_xlsx(f3, sheet_name, header_row)

    loaded_dfs, loaded_names = [], []
    all_dfs = [df1, df2, df3]
    all_names = ["Файл_1", "Файл_2", "Файл_3"]
    for df, name in zip(all_dfs, all_names):
        if df is not None:
            loaded_dfs.append(df)
            loaded_names.append(name)

    if loaded_dfs:
        st.info(f"Загружено файлов: {len(loaded_dfs)}")
        preview_tabs = st.tabs(loaded_names)
        for tab, df, name in zip(preview_tabs, loaded_dfs, loaded_names):
            with tab:
                st.write(f"{name}: {df.shape[0]} строк × {df.shape[1]} колонок")
                st.dataframe(df.head(50), use_container_width=True)

    if len(loaded_dfs) < 2:
        st.warning("⚠️ Загрузите минимум два файла для объединения")
    else:
        st.divider()
        st.success(f"✅ Загружено {len(loaded_dfs)} файлов. Готово к объединению!")

        st.subheader("Шаг 2: Настройка ключевого поля")
        st.write("Выберите колонку-ключ в каждом файле.")
        result_key_name = st.text_input("Имя ключа в результате", value="Ключ")
        key_cols = []
        key_select_cols = st.columns(len(loaded_dfs))
        for idx, (col, df, name) in enumerate(zip(key_select_cols, loaded_dfs, loaded_names)):
            with col:
                key = st.selectbox(f"Ключ в {name}", options=list(df.columns), index=0, key=f"key_{idx}")
                key_cols.append(key)
        st.divider()

        st.subheader("Шаг 3: Выбор дополнительных полей")
        st.write("Выберите поля из каждого файла (кроме ключа). Они получат префиксы [1]/[2]/[3].")
        add_cols = []
        field_tabs = st.tabs([f"Поля {name}" for name in loaded_names])
        for idx, (tab, df, name, key_col) in enumerate(zip(field_tabs, loaded_dfs, loaded_names, key_cols)):
            with tab:
                available_cols = [c for c in df.columns if c != key_col]
                selected = st.multiselect(f"Выберите поля из {name}", options=available_cols, default=[], key=f"fields_{idx}")
                add_cols.append(selected)
        st.divider()

        def build_merged_result(dfs_list, key_cols_list, add_cols_list, names_list, result_key_name, join_type, do_normalize):
            prepared_dfs = []
            for idx, (df, key_col, extra_cols, name) in enumerate(zip(dfs_list, key_cols_list, add_cols_list, names_list)):
                cols = [key_col] + extra_cols
                temp_df = df[cols].copy()
                if do_normalize:
                    temp_df[key_col] = normalize_series_key(temp_df[key_col])
                temp_df = temp_df.rename(columns={key_col: result_key_name})
                prefix = f"[{idx+1}] "
                temp_df = temp_df.rename(columns={col: f"{prefix}{col}" for col in temp_df.columns if col != result_key_name})
                prepared_dfs.append(temp_df)
            result = prepared_dfs[0]
            for next_df in prepared_dfs[1:]:
                result = pd.merge(result, next_df, on=result_key_name, how=join_type)
            if join_type == "outer":
                non_key_cols = [c for c in result.columns if c != result_key_name]
                result = result.dropna(subset=non_key_cols, how='all')
            return result

        st.subheader("Шаг 4: Создание результата")
        if st.button("🚀 Объединить и создать Excel", use_container_width=True):
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
                with st.expander("👁️ Предпросмотр результата (первые 100 строк)"):
                    st.dataframe(result.head(100), use_container_width=True)
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                    for df, name in zip(loaded_dfs, loaded_names):
                        df.to_excel(writer, sheet_name=name, index=False)
                    result.to_excel(writer, sheet_name="Merge", index=False)
                    for sheet_name, worksheet in writer.sheets.items():
                        ws_df = result if sheet_name == "Merge" else loaded_dfs[loaded_names.index(sheet_name)]
                        worksheet.set_column(0, max(0, ws_df.shape[1] - 1), 18)
                bio.seek(0)
                st.download_button(
                    label="📥 Скачать Excel файл",
                    data=bio,
                    file_name=output_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                st.session_state["last_merged_result"] = result

            except Exception as e:
                st.error(f"❌ Ошибка при объединении: {e}")
                st.exception(e)

# ======================
# --- TAB2: КАТАЛОГ/BД ---
# ======================
with tab2:
    st.title("Каталог товаров (постоянная база SQLite)")
    df = load_all_products()

    st.subheader("Список товаров:")
    st.dataframe(df, use_container_width=True)

    with st.form("add_form"):
        st.write("Добавить товар")
        name = st.text_input("Название")
        price = st.number_input("Цена", step=0.01)
        code = st.text_input("Код")
        country = st.text_input("Страна")
        description = st.text_area("Описание")
        submitted = st.form_submit_button("Добавить")
        if submitted:
            add_product({"name": name, "price": price, "code": code, "country": country, "description": description})
            st.success("Добавлено!")
            st.experimental_rerun()

    st.subheader("Редактировать/Удалить товар")
    select_id = st.selectbox("Выберите ID товара для редактирования", df['id'].tolist() if len(df) else [])
    if select_id:
        row = df[df['id'] == select_id].iloc[0]
        with st.form("edit_form"):
            name = st.text_input("Название", value=row['name'])
            price = st.number_input("Цена", value=row['price'], step=0.01)
            code = st.text_input("Код", value=row['code'])
            country = st.text_input("Страна", value=row['country'])
            description = st.text_area("Описание", value=row['description'])
            saved = st.form_submit_button("Сохранить изменения")
            if saved:
                update_product(select_id, {"name": name, "price": price, "code": code, "country": country, "description": description})
                st.success("Обновлено!")
                st.experimental_rerun()
            delete = st.form_submit_button("Удалить товар")
            if delete:
                delete_product(select_id)
                st.success("Удалено!")
                st.experimental_rerun()

    # Если есть объединённый результат из tab1 — добавить массово в базу
    if "last_merged_result" in st.session_state and st.button("Импортировать объединённые строки в каталог (SQLite)"):
        result_df = st.session_state["last_merged_result"]
        for _, row in result_df.iterrows():
            add_product({
                "name": str(row.get("Артикул", row.get("Название", ""))),
                "price": float(row.get("Цена", 0)) if "Цена" in row else None,
                "code": str(row.get("Код", "")),
                "country": str(row.get("Страна", "")),
                "description": str(row.get("Описание", "")),
            })
        st.success(f"Импортировано: {len(result_df)} строк!")
        st.experimental_rerun()

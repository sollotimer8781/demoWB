import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Merge Excel файлов", layout="wide")
st.title("Объединение Excel файлов по ключевому полю")
st.caption("Загрузите 2 или 3 Excel файла, выберите ключ для объединения. Результат содержит только строки, найденные во ВСЕХ файлах.")

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
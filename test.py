from main import SQLTable

db_config = {
    'host': 'srv221-h-st.jino.ru',
    'user': 'j30084097_13418',
    'password': 'pPS090207/()',
    'database': 'j30084097_13418',
    'port': 3306
}

if __name__ == "__main__":
    TBL_BOOKS = 'test_books'
    TBL_AUTHORS = 'test_authors'

    db = SQLTable(db_config, TBL_BOOKS, engine='mysql')

    db.drop_table()
    db.create_table('id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(100), price INT')

    db.insert({'title': 'Мастер и Маргарита', 'price': 650})
    db.insert({'title': 'Преступление и наказание', 'price': 720})
    db.insert({'title': 'Война и мир', 'price': 890})
    db.insert({'title': 'Евгений Онегин', 'price': 540})

    print("\nКниги с ценой > 600:")
    for book in db.select(filters={'price': ('>', 600)}):
        print(book)

    db_authors = SQLTable(db_config, TBL_AUTHORS, engine='mysql')
    db_authors.drop_table()
    db_authors.create_table('id INT AUTO_INCREMENT PRIMARY KEY, book_id INT, author_name VARCHAR(100)')

    db_authors.insert({'book_id': 1, 'author_name': 'Михаил Булгаков'})
    db_authors.insert({'book_id': 2, 'author_name': 'Фёдор Достоевский'})
    db_authors.insert({'book_id': 3, 'author_name': 'Лев Толстой'})
    db_authors.insert({'book_id': 4, 'author_name': 'Александр Пушкин'})

    print("\nINNER JOIN (книги + авторы):")
    join_res = db.join_query(
        TBL_AUTHORS,
        f'{TBL_BOOKS}.id = {TBL_AUTHORS}.book_id',
        columns=f'{TBL_BOOKS}.title, {TBL_AUTHORS}.author_name',
        join_type='INNER'
    )
    for r in join_res:
        print(r)

    print("\nUNION (названия книг + имена авторов):")
    q1 = (f"SELECT title as value FROM {TBL_BOOKS}", [])
    q2 = (f"SELECT author_name as value FROM {TBL_AUTHORS}", [])
    union_res = db.union_query([q1, q2], distinct=True)
    for r in union_res:
        print(r)

    db_authors.drop_table()
    db.drop_table()
    db.disconnect()
    db_authors.disconnect()

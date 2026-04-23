from main import SQLTable


if __name__ == "__main__":
    books_db = SQLTable(db_config, 'library_books', engine='mysql')
    authors_db = SQLTable(db_config, 'library_authors', engine='mysql')

    books_db.drop_table()
    authors_db.drop_table()

    print('создание таблиц')
    authors_db.create_table('id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), country VARCHAR(50)')
    books_db.create_table('id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(100), author_id INT, year INT, price INT')

    print('добавление авторов и книг')
    authors_db.insert({'name': 'Александр Пушкин', 'country': 'Россия'})
    authors_db.insert({'name': 'Джордж Оруэлл', 'country': 'Великобритания'})
    authors_db.insert({'name': 'Стивен Кинг', 'country': 'США'})

    books_db.insert({'title': 'Капитанская дочка', 'author_id': 1, 'year': 1836, 'price': 500})
    books_db.insert({'title': '1984', 'author_id': 2, 'year': 1949, 'price': 700})
    books_db.insert({'title': 'Сияние', 'author_id': 3, 'year': 1977, 'price': 600})
    books_db.insert({'title': 'Евгений Онегин', 'author_id': 1, 'year': 1833, 'price': 450})

    # фильтрация
    print("\nфильтрация( книги с ценой > 550)")
    expensive_books = books_db.select(filters={'price': ('>', 550)})
    for book in expensive_books:
        print(f"Книга: {book['title']}, Цена: {book['price']}")

    # 5. сложный запрос JOIN (книги вместе с именами авторов)
    print("\nJOIN(список книг с их авторами)")
    # Соединяем library_books и library_authors
    join_res = books_db.join_query(
        other_table='library_authors',
        on='library_books.author_id = library_authors.id',
        columns='library_books.title, library_authors.name as author',
        join_type='INNER'
    )
    for row in join_res:
        print(f"'{row['title']}' написал {row['author']}")

    # 6. UNION (объединие названия книг и имен авторов в один список)
    print("\nUNION(все названия и имена в базе)")
    q1 = ("SELECT title as value FROM library_books", [])
    q2 = ("SELECT name as value FROM library_authors", [])
    union_res = books_db.union_query([q1, q2], union_all=False)
    for item in union_res:
        print(f"Объект: {item['value']}")

    # Завершение
    books_db.disconnect()
    authors_db.disconnect()
    print("\nтест успешно завершен.")

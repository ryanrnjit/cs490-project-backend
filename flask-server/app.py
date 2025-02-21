from flask import Flask, request, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql.functions import func
from sqlalchemy import desc, text
from sqlalchemy.dialects import mysql

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sakila.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

@app.route("/")
def home():
    return "<h1>It Works!</h1>"

@app.route("/customerlist")
def customerlist():
    query = text("""
        SELECT CL.ID, CL.name, CL.address, CL.zip_code, CL.phone, CL.city, CL.country, CL.notes, CL.SID, C.email
        FROM customer_list AS CL
        INNER JOIN customer AS C ON C.customer_id = CL.ID
    """)
    result = db.session.execute(query)
    json = {'customers':[]}
    for row in result:
        json['customers'].append({
            'name': row.name,
            'customer_id': row.ID,
            'address': row.address,
            'zip_code': str(row.zip_code).zfill(5),
            'city': row.city,
            'phone': row.phone,
            'country': row.country,
            'store_id': row.SID,
            'email': row.email,
        })
    return json

@app.route("/topfiveactors")
def topfiveactors():
    query = text("""
        SELECT A.actor_id, (A.first_name || " " || A.last_name) as 'actor_name', COUNT(FA.film_id) as 'film_count'
        FROM actor AS A
        INNER JOIN film_actor AS FA
            ON FA.actor_id = A.actor_id
        GROUP BY A.actor_id
        ORDER BY film_count DESC
        LIMIT 5;         
    """)
    result = db.session.execute(query)
    json = {'actors': []}
    for row in result:
        json['actors'].append({
            'actor_id': row.actor_id,
            'actor_name': row.actor_name,
            'film_count': row.film_count,
        })
    return json
        
@app.route("/search", methods=['GET', 'POST'])
def search():
    json = {'result_count': 0, 'films':[]}
    if(request.args.get('search') == None or request.args.get('search') == ''): return json
    query = text("""
        SELECT F.film_id, F.title, group_concat((A.first_name || ' ' || A.last_name)) AS actor_names, C.name
        FROM film AS F
        INNER JOIN film_actor AS FA ON F.film_id = FA.film_id
        INNER JOIN actor as A ON A.actor_id = FA.actor_id
        INNER JOIN film_category AS FC ON FC.film_id = F.film_id
        INNER JOIN category AS C ON C.category_id = FC.category_id
        GROUP BY F.film_id
        HAVING F.title LIKE :title
            OR actor_names LIKE :actors
            OR C.name LIKE :name
    """)
    #print(query)
    result = db.session.execute(query, {
        'title':'%' + request.args['search'] + '%',
        'actors':'%' + request.args['search'] + '%',
        'name':'%' + request.args['search'] + '%',
        })
    for row in result:
        json['result_count'] += 1
        json['films'].append({
            'film_id': row.film_id,
            'title': row.title,
            'actor_names': row.actor_names,
            'category_name': row.name,
        })
    return json

@app.route("/actordetails", methods=['GET', 'POST'])
def actordetails():
    if(request.args.get('actor_id') == None): return Response(status=400)
    query = text(f"""
        SELECT I.film_id, F.title, COUNT(I.film_id) AS 'rental_count'
        FROM `rental` AS R
        INNER JOIN `inventory` AS I
            ON I.inventory_id = R.inventory_id
        INNER JOIN `film` AS F
            ON F.film_id = I.film_id
        INNER JOIN `film_actor` AS FA
            ON FA.film_id = I.film_id
        WHERE FA.actor_id = {request.args['actor_id']}
        GROUP BY I.film_id
        ORDER BY rental_count DESC
        LIMIT 5;             
    """)
    result = db.session.execute(query)
    #row = result.first()
    json = {
        'films':[]
    }
    for row in result:
        json['films'].append({
            'film_id': row.film_id,
            'title': row.title,
            'rental_count': row.rental_count,
        })
    return json

@app.route("/filmdetails", methods=['GET', 'POST'])
def filmdetails():
    if(request.args.get('film_id') == None): return Response(status=400)
    query = text(f"""
        SELECT F.film_id, F.title, F.description, F.release_year, F.rental_duration, F.rental_rate, F.length, F.replacement_cost, F.rating, F.special_features, C.name AS genre, FC.category_id
        FROM `film` AS F
        INNER JOIN `film_category` AS FC
            ON F.film_id = FC.film_id
        INNER JOIN `category` AS C
            ON C.category_id = FC.category_id
        WHERE F.film_id = {request.args['film_id']};
    """)
    result = db.session.execute(query)
    row = result.first()
    #print(result)
    json = {
        'film_id': row.film_id,
        'title': row.title,
        'description': row.description,
        'release_year': row.release_year,
        'rental_duration': row.rental_duration,
        'rental_rate': row.rental_rate,
        'length': row.length,
        'replacement_cost': row.replacement_cost,
        'rating': row.rating,
        'special_features': row.special_features,
        'genre': row.genre,
    }
    return json

@app.route("/topfivefilms")
def topfivefilms():
    query = text("""
        SELECT I.film_id, F.title, COUNT(I.film_id) AS 'rental_count'
        FROM `rental` AS R
        INNER JOIN `inventory` AS I
            ON I.inventory_id = R.inventory_id
        INNER JOIN `film` AS F
            ON F.film_id = I.film_id
        GROUP BY I.film_id
        ORDER BY rental_count DESC
        LIMIT 5;
    """)
    listoffilms = db.session.execute(query)
    json = {'films': []}
    for film_id, title, count in listoffilms:
        json['films'].append({
            'film_id': film_id,
            'title': title,
            'rental_count': count,
        })
    return json

@app.route("/films")
def films():
    query = text("""
        SELECT * FROM film;
    """)
    listoffilms = db.session.execute(query)
    json = {'films': []}
    for film in listoffilms:
        json['films'].append({
            'id': film.film_id,
            'title': film.title,
            'description': film.description,
            'release_year': film.release_year,
            'language_id': film.language_id,
            'original_language_id': film.original_language_id,
            'rental_duration': film.rental_duration,
            'rental_rate': film.rental_rate,
            'length': film.length,
            'replacement_cost': film.replacement_cost,
            'rating': film.rating,
            'special_features': film.special_features,
            'last_update': film.last_update
        })
    return json

if __name__ == "__main__":
    app.run(debug=True)
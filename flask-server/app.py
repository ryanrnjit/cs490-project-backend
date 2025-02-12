from flask import Flask, request, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql.functions import func
from sqlalchemy import desc, text
from sqlalchemy.dialects import mysql

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sakila.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Rental(db.Model):
    __tablename__ = "rental"
    rental_id = db.Column(db.INTEGER, primary_key=True)
    rental_date = db.Column(db.DATETIME)
    inventory_id = db.Column(db.Integer, db.ForeignKey("inventory.inventory_id"))
    customer_id = db.Column(db.SMALLINT)
    return_date = db.Column(db.DATETIME)
    staff_id = db.Column(db.Integer)

class FilmActor(db.Model):
    __tablename__ = "film_actor"
    actor_id = db.Column(db.SMALLINT, primary_key = True)
    film_id = db.Column(db.SMALLINT, db.ForeignKey("film.film_id"), primary_key = True)
    last_update = db.Column(db.TIMESTAMP)

class Inventory(db.Model):
    __tablename__ = "inventory"
    inventory_id = db.Column(db.Integer, primary_key = True)
    film_id = db.Column(db.SMALLINT, db.ForeignKey("film.film_id"))
    store_id = db.Column(db.Integer)
    last_update = db.Column(db.TIMESTAMP)

class Film(db.Model):
    __tablename__ = "film"
    film_id = db.Column(db.SMALLINT, primary_key=True)
    title = db.Column(db.VARCHAR(128))
    description = db.Column(db.TEXT())
    release_year = db.Column(db.Date())
    language_id = db.Column(db.SMALLINT())
    original_language_id = db.Column(db.SMALLINT())
    rental_duration = db.Column(db.SMALLINT())
    rental_rate = db.Column(db.DECIMAL(4,2))
    length = db.Column(db.SMALLINT())
    replacement_cost = db.Column(db.DECIMAL(5,2))
    rating = db.Column(db.Enum('G', 'PG', 'PG-13', 'R', 'NC-17'))
    special_features = db.Column(mysql.SET('Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes'))
    last_update = db.Column(db.TIMESTAMP())
    
    def __repr__(self):
        return f'<Film {self.film_id}, {self.title}>'

@app.route("/")
def home():
    return "<h1>It Works!</h1>"

@app.route("/topfiveactors")
def topfiveactors():
    query = text("""
        SELECT A.actor_id, CONCAT(A.first_name, " ", A.last_name) as 'actor_name', COUNT(FA.film_id) as 'film_count'
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
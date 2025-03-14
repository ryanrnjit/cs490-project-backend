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
    query_text = """
        SELECT CL.ID, CL.name, CL.address, CL.zip_code, CL.phone, CL.city, CL.country, CL.notes, CL.SID, C.email
        FROM customer_list AS CL
        INNER JOIN customer AS C ON C.customer_id = CL.ID
    """
    search_addition = ""
    search_term = request.args.get('search_term')
    if(request.args.get('search_type') == '1'): #id
        search_addition = "WHERE CL.ID = :search_term"
    elif(request.args.get('search_type') == '2'): #first name
        search_term = "%" + search_term + "%"
        search_addition = "WHERE C.first_name LIKE :search_term"
    elif(request.args.get('search_type') == '3'): #last name
        search_term = "%" + search_term + "%"
        search_addition = "WHERE C.last_name LIKE :search_term"
    query = text(query_text + search_addition)
    result = db.session.execute(query, {'search_term': search_term})
    print(query)
    print(search_term)
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

@app.route("/instock", methods=['GET'])
def instock():
    if(request.args.get('film_id') == None): return Response(status=400)
    query = text("SELECT inventory_id, film_id FROM inventory WHERE inventory_id NOT IN ( SELECT inventory_id FROM rental WHERE return_date IS NULL ) AND film_id = :fid")
    result = db.session.execute(query, {'fid': request.args.get('film_id')})
    json = {
        'count': 0,
        'in_stock': []
        }
    for row in result:
        json['count'] += 1
        json['in_stock'].append({'inventory_id': row.inventory_id})
    return json

@app.route("/countries")
def countries():
    query = text("SELECT * FROM country")
    result = db.session.execute(query)
    json = {
        'countries': []
    }
    for row in result:
        json['countries'].append({
            'id': row.country_id,
            'country': row.country
        })
    return json

@app.route("/deletecustomer/<int:customer_id>", methods=['DELETE'])
def deletecustomer(customer_id):
    if(customer_id == None):
        return {'message': 'Customer ID not provided'}, 400
    try:
        db.session.execute(text("DELETE FROM customer WHERE customer_id = :cid"), {'cid': customer_id})
    except Exception as e:
        print(e)
        return {'message': 'Server error.'}, 500
    else:
        db.session.commit()
        return {'message': f"User successfully deleted (id: {customer_id})"}

@app.route("/customerrentals/<int:customer_id>", methods=['GET'])
def customerrentals(customer_id):
    query = text("""
        SELECT * FROM rental WHERE customer_id = :cid ORDER BY rental_id DESC
    """)
    result = db.session.execute(query, {'cid': customer_id})
    json = {'rentals': []}
    for row in result:
        json['rentals'].append({
            'rental_id': row.rental_id,
            'rental_date': row.rental_date,
            'inventory_id': row.inventory_id,
            'customer_id': customer_id,
            'return_date': row.return_date
        })
    return json

@app.route("/returnfilm/<int:rental_id>", methods=['PATCH'])
def returnfilm(rental_id):
    if(rental_id == None):
        return {'message': 'Rental ID not provided'}, 400
    try:
        db.session.execute(text("UPDATE rental SET return_date = CURRENT_TIMESTAMP WHERE rental_id = :rid"), {'rid': rental_id})
    except Exception as e:
        print(e)
        return {'message': 'Server error.'}, 500
    else:
        db.session.commit()
        return {'message': f"Successfully returned rental film (rental id: {rental_id})"}, 200
    

@app.route("/getcustomer/<int:customer_id>", methods=['GET'])
def getcustomer(customer_id):
    query = text("""
        SELECT C.customer_id, C.first_name, C.last_name, C.email, C.address_id, A.address, A.address2, A.district, A.city_id, A.postal_code, A.phone, CI.city, CI.country_id, C.create_date, C.last_update, CNTR.country
        FROM customer AS C
        INNER JOIN address AS A ON C.address_id = A.address_id
        INNER JOIN city AS CI ON A.city_id = CI.city_id
        INNER JOIN country AS CNTR ON CNTR.country_id = CI.country_id
        WHERE C.customer_id = :cid
    """)
    result = db.session.execute(query, {'cid': customer_id})
    row = result.first()
    return {
        'customer_id': row.customer_id,
        'first_name': row.first_name,
        'last_name': row.last_name,
        'email': row.email,
        'address_id': row.address_id,
        'address': row.address,
        'address2': row.address2,
        'district': row.district,
        'city_id': row.city_id,
        'city': row.city,
        'postal_code': row.postal_code,
        'phone': row.phone,
        'country_id': row.country_id,
        'create_date': row.create_date,
        'country': row.country,
        'last_update': row.last_update
    }
        
    

@app.route("/editcustomer", methods=['PATCH'])
def editcustomer():
    if(request.json == None):
        return 400
    json = request.json
    country_id = json.get('country_id')
    city = json.get('city')
    address = json.get('address')
    address2 = json.get('address2')
    district = json.get('district')
    postal_code = json.get('postal_code')
    phone = json.get('phone')
    first_name = json.get('first_name').upper()
    last_name = json.get('last_name').upper()
    email = json.get('email')
    customer_id = json.get('customer_id')
    city_id = json.get('city_id')
    address_id = json.get('address_id')
    if(country_id == None or city == None or address == None or postal_code == None or first_name == None or last_name == None or email == None):
        return {'message': 'Required inputs were not provided.'}, 400
    query_city = text("""
        UPDATE city SET
            city = :city,
            country_id = :country_id,
            last_update = CURRENT_TIMESTAMP
            WHERE city_id = :city_id
    """)
    query_address = text("""
        UPDATE address SET
            address = :address,
            address2 = :address2,
            district = :district,
            postal_code = :zipcode,
            phone = :phone,
            last_update = CURRENT_TIMESTAMP
        WHERE address_id = :address_id
    """)
    query_customer = text("""
        UPDATE customer SET
            first_name = :firstname,
            last_name = :lastname,
            email = :email,
            address_id = :address_id,
            last_update = CURRENT_TIMESTAMP
        WHERE customer_id = :customer_id
    """)
    try:
        db.session.execute(query_city, {'city': city, 'country_id': country_id, 'city_id': city_id})
        db.session.execute(query_address, {'address': address, 'address_id': address_id, 'address2': address2, 'district': district, 'zipcode': postal_code, 'phone': phone})
        db.session.execute(query_customer, {'firstname': first_name, 'lastname': last_name, 'email': email, 'address_id': address_id, 'customer_id': customer_id})
    except Exception as e:
        print(e)
        return {'message': 'Server error. Please try again later.'}, 500
    else:
        db.session.commit()
        return {'message': f"Success. Customer {first_name} {last_name} updated (id: {customer_id})."}, 200

@app.route("/createcustomer", methods=['POST'])
def createcustomer():
    if(request.json == None):
        return 400
    json = request.json
    country_id = json.get('country_id')
    city = json.get('city')
    address = json.get('address')
    address2 = json.get('address2')
    district = json.get('district')
    postal_code = json.get('postal_code')
    phone = json.get('phone')
    first_name = json.get('first_name').upper()
    last_name = json.get('last_name').upper()
    email = json.get('email')
    customer_id = None
    if(country_id == None or city == None or address == None or postal_code == None or first_name == None or last_name == None or email == None):
        return {'message': 'Required inputs were not provided.'}, 400
    query_city = text("""
    INSERT INTO city (city_id, city, country_id, last_update)
    VALUES (
        (SELECT MAX(city_id) FROM city) + 1,
        :city,
        :country_id,
        CURRENT_TIMESTAMP
    );
    """)

    query_address = text("""INSERT INTO address (address_id, address, address2, district, city_id, postal_code, phone, last_update)
    VALUES (
        (SELECT MAX(address_id) FROM address) + 1,
        :address,
        :address2,
        :district,
        (SELECT city_id FROM city WHERE city = :city),
        :zipcode,
        :phone,
        CURRENT_TIMESTAMP
    );
    """)

    query_customer = text("""INSERT INTO customer (customer_id, store_id, first_name, last_name, email, address_id, active, create_date, last_update)
    VALUES (
        (SELECT MAX(customer_id) FROM customer) + 1,
        1,
        :firstname,
        :lastname,
        :email,
        (SELECT address_id FROM address WHERE address = :address),
        1,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );
    """)
    try:
        db.session.execute(query_city, {'city': city, 'country_id': country_id})
        db.session.execute(query_address, {'address': address, 'address2': address2, 'district': district, 'city':city, 'zipcode': postal_code, 'phone': phone})
        db.session.execute(query_customer, {'firstname': first_name, 'lastname': last_name, 'email': email, 'address': address})
    except Exception as e:
        print(e)
        return {'message': 'Server error. Please try again later.'}, 500
    else:
        db.session.commit()
        result = db.session.execute(text("SELECT MAX(customer_id) as latest_customer FROM customer"))
        row = result.first()
        customer_id = row.latest_customer
        print(customer_id)
        return {'message': f"Success. Customer {first_name} {last_name} added (id: {customer_id})."}, 200
    

@app.route("/rentfilm", methods=['POST'])
def rentfilm():
    if(request.json == None):
        return Response(status=400) #bad request
    json = request.json
    inventory_id = json.get('inventory_id')
    customer_id = json.get('customer_id')
    staff_id = json.get('staff_id')
    if(inventory_id == None or customer_id == None or staff_id == None):
        return Response(status=400) #bad request
    
    #input validation
    result = db.session.execute(text("SELECT customer_id FROM customer WHERE customer_id == :cid"), {'cid': customer_id})
    if(result.first() == None): return {'message': 'Invalid Customer ID'}, 400
    result = db.session.execute(text("SELECT staff_id FROM staff WHERE staff_id == :sid"), {'sid': staff_id})
    if(result.first() == None): return {'message': 'Invalid Staff ID'}, 400
    
    query1 = text("""
    INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
    VALUES ( 
        (SELECT MAX(rental_id) FROM rental) + 1,
        CURRENT_TIMESTAMP,
        :iid,
        :cid,
        NULL,
        :sid,
        CURRENT_TIMESTAMP 
    )
    """)
    query2 = text("""   
    INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date, last_update)
    VALUES (
        (SELECT MAX(payment_id) FROM payment) + 1,
        :cid,
        :sid,
        (SELECT MAX(rental_id) FROM rental),
        (SELECT rental_rate FROM film WHERE film_id = (SELECT film_id FROM inventory WHERE inventory_id = :iid)),
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    )
                  """)
    try:
        db.session.execute(query1, {'cid': customer_id, 'sid': staff_id, 'iid': inventory_id})
        db.session.execute(query2, {'cid': customer_id, 'sid': staff_id, 'iid': inventory_id})
    except:
        return Response(status=500)
    else:
        db.session.commit()
    finally:
        return {'message': 'Inventory ID ' + str(inventory_id) + ' successfully rented to Customer ID ' + str(customer_id)}
        
#@app.route("/searchcustomer/<string:first_name>/<string:last_name>/<int:customer_id>")
#def searchcustomer(first_name, last_name, customer_id):
    

@app.route("/search", methods=['GET'])
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

@app.route("/actordetails", methods=['GET'])
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

@app.route("/filmdetails", methods=['GET'])
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
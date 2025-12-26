use mflix

db.comments.findOne()

// Insertar 5 nuevos usuarios en la colección users.
// Para cada nuevo usuario creado, insertar al menos un comentario realizado por el usuario en la colección comments.
db.users.findOne()
// hecho por salvi amor
db.users.insertMany([
    {"email": "simpons@gmail.com", "name": "Bart", "password": "hackeame_gg"},
    {"email": "naruto@gmail.com", "name": "Naruto Uzumaki", "password": "ramen123"},
    {"email": "sasuke@gmail.com", "name": "Sasuke Uchiha", "password": "narutoteamo"},
    {"email": "sakura@gmail.com", "name": "Sakura Haruno", "password": "sasuketeamo"},
    {"email": "kakashi@gmail.com", "name": "Kakashi Hatake", "password": "obitoteamo"}
])

db.comments.insertOne({
    "date": new Date(),
    "email": "simpons@gmail.com",
    "name": "Bart",
    "movie_id": "573a1390f29313caabcd418c",
    "text": "una joia"
})
db.comments.insertMany([
    { "date": new Date(),"email": "naruto@gmail.com","name": "Naruto Uzumaki","movie_id": "573a1390f29313caabcd418c","text": "una joia"},
    { "date": new Date(),"email": "sasuke@gmail.com", "name": "Sasuke Uchiha","movie_id": "573a1390f29313caabcd418c","text": "una joia"},
    { "date": new Date(),"email": "sakura@gmail.com", "name": "Sakura Haruno","movie_id": "573a1390f29313caabcd418c","text": "una joia"},
    { "date": new Date(),"email": "kakashi@gmail.com", "name": "Kakashi Hatake","movie_id": "573a1390f29313caabcd418c","text": "una joia"}
])

// Listar el título, año, actores (cast), directores y rating de las 10 películas con mayor rating (“imdb.rating”) de la década del 90.
// ¿Cuál es el valor del rating de la película que tiene mayor rating? (Hint: Chequear que el valor de “imdb.rating” sea de tipo “double”).

db.movies.find(
    {"year": {$gte: 1990, $lte: 1999}, "imdb.rating": {$type: "double"}},
    {"title": 1, "year": 1, "cast": 1, "directors": 1, "imdb.rating": 1}
).sort({"imdb.rating": -1}).limit(10)

// Listar el nombre, email, texto y fecha de los comentarios que la película con id (movie_id) ObjectId("573a1399f29313caabcee886") recibió entre los años 2014 y 2016 inclusive.
// Listar ordenados por fecha. Escribir una nueva consulta (modificando la anterior) para responder ¿Cuántos comentarios recibió?

db.comments.find(
    {"movie_id": ObjectId("573a1399f29313caabcee886"), $expr: {$in:[ {$year:"$date"} , [2014,2015,2016]]} },
    {"name":1, "email":1, "text":1, "date": 1}
).sort({"date": 1})

db.comments.find(
    {"movie_id": ObjectId("573a1399f29313caabcee886"), $expr: {$in:[ {$year:"$date"} , [2014,2015,2016]]} },
    {"name":1, "email":1, "text":1, "date": 1}
).sort({"date": 1}).count() // 34

// Listar el nombre, id de la película, texto y fecha de los 3 comentarios más recientes realizados por el usuario con email patricia_good@fakegmail.com.

db.comments.find(
    {email:"patricia_good@fakegmail.com"},
    {name:1, movie_id:1,text:1,date:1}
).sort({date:-1})

//Listar el título, idiomas (languages), géneros, fecha de lanzamiento (released) y número de votos
//(“imdb.votes”) de las películas de géneros Drama y Action (la película puede tener otros géneros adicionales),
//que solo están disponibles en un único idioma y por último tengan un rating (“imdb.rating”) mayor a 9 o bien tengan una duración (runtime) de al menos 180 minutos.
//Listar ordenados por fecha de lanzamiento y número de votos.

db.movies.find(
    {genres:{$all:["Drama", "Action"]}, countries:{$size:1}, $expr: { $or:[{ "imdb.rating":{$gte:9}} , {runtime:{$gte:3}}]}},
    {title:1,countries:1,genres:1,released:1,"imdb.votes":1}
)

// Listar el id del teatro (theaterId), estado (“location.address.state”), ciudad (“location.address.city”),
// y coordenadas (“location.geo.coordinates”) de los teatros que se encuentran en algunos de los estados "CA", "NY", "TX"
// y el nombre de la ciudades comienza con una ‘F’. Listar ordenados por estado y ciudad.

db.theaters.find(
    {
        "location.address.state":{$in:["CA","NY","TX"]},
        "location.address.city": {$regex: "^F.*"},
    },
    {theaterId:1, "location.address.state":1, "location.address.city":1, "location.geo.coordinates":1}
).sort({"location.address.state":1, "location.address.city":1})

// Actualizar los valores de los campos texto (text) y fecha (date) del comentario cuyo id es ObjectId("5b72236520a3277c015b3b73")
// a "mi mejor comentario" y fecha actual respectivamente.

db.comments.updateOne(
    {"_id":ObjectId("5b72236520a3277c015b3b73")},
    {
         $set: {
             text: "mi mejor comentario",
             date: new Date()
        }
    }
)

// Actualizar el valor de la contraseña del usuario cuyo email es joel.macdonel@fakegmail.com a "some password".
// La misma consulta debe poder insertar un nuevo usuario en caso que el usuario no exista.
// Ejecute la consulta dos veces. ¿Qué operación se realiza en cada caso?  (Hint: usar upserts).

db.users.updateOne(
    {email: "joel.macdonel@fakegmai.com"},
    {
        $set: {
            password: "some password again"
        }
    },
    {upsert: true}
)

// Remover todos los comentarios realizados por el usuario cuyo email es victor_patel@fakegmail.com durante el año 1980.

db.comments.deleteMany(
    {email: "victor_patel@fakegmail.com", $expr: {$eq: [{$year: "$date"}, 1981]} }
)



// Cantidad de cines (theaters) por estado.

db.theaters.aggregate([
    {$group:{_id:"$location.address.state",count:{$sum:1}}}
])

// Cantidad de estados con al mas de 4 cines (theaters) registrados.

db.theaters.aggregate([
    {$group:{_id:"$location.address.state",count:{$sum:1}}},
    {$match:{count: {$gte:4}}},
    {$count:"state_>=4_theaters"}
])

// Cantidad de películas dirigidas por "Louis Lumière". Se puede responder sin pipeline de agregación, realizar ambas queries.

db.movies.find(
    {directors:{$all:['Louis Lumière']}}
)
.count()

db.movies.aggregate([
    {$match:{directors:{$all:['Louis Lumière']}}},
    {$count:"luis"}
])

// Cantidad de películas estrenadas en los años 50 (desde 1950 hasta 1959). Se puede responder sin pipeline de agregación, realizar ambas queries.

db.movies.find(
    {year:{$gte:1950, $lt:1960}}
)
.count()

db.movies.aggregate([
    {$match:{year:{$gte:1950, $lt:1960}}},
    {$count:"50s"}
])

// Listar los 10 géneros con mayor cantidad de películas (tener en cuenta que las películas pueden tener más de un género).
// Devolver el género y la cantidad de películas. Hint: unwind puede ser de utilidad

db.movies.aggregate([
    {$unwind:"$genres"},
    {$group:{_id:"$genres",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:10},
    {$project:{genres:1,count:1}}
])

// Top 10 de usuarios con mayor cantidad de comentarios, mostrando Nombre, Email y Cantidad de Comentarios.

db.comments.aggregate([
    {$group:{_id:"$email", count:{$sum:1},name:{$first:"$name"}}},
    {$sort:{count:-1}},
    {$limit:10},
])

db.comments.aggregate([
    {$group:{_id:{email:"$email",name:"$name"}, count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:10},
])

// Ratings de IMDB promedio, mínimo y máximo por año de las películas estrenadas en los años 80
// (desde 1980 hasta 1989), ordenados de mayor a menor por promedio del año.

db.movies.aggregate([
    {$match:{year:{$gte:1980,$lt:1990},"imdb.rating": {$type: "double"}}},
    {
        $group:{
            _id:"$year",
            promedio:{$avg:"$imdb.rating"},
            minimo:{$min:"$imdb.rating"},
            maximo:{$max:"$imdb.rating"}
        }
    },
    {$sort:{promedio:-1}}
])

// Título, año y cantidad de comentarios de las 10 películas con más comentarios.

db.comments.aggregate([
    {
        $group:{
            _id:"$movie_id",
            count:{$sum:1}
        }
    },
    {$sort:{count:-1}},
    {$limit:10},
    {
        $lookup:{
            from: "movies",
            localField:"_id",
            foreignField:"_id",
            as:"movie"
        }
    },
    {$unwind: "$movie"},
    {$project:{_id:0,title:"$movie.title",year:"$movie.year",count:1}}
])

db.movies.aggregate([
    {$lookup:{
        from:"comments",
        localField:"_id",
        foreignField:"movie_id",
        as: "comments"
    }},
    {
        $addFields: {
            comments_count: { $size: "$comments" }
        }
    },
    {$limit:10},
    {$sort: { comments_count: -1 }},
    {$project:{_id:0,title:1,year:1,comments_count:1}}
])

// Crear una vista con los 5 géneros con mayor cantidad de comentarios, junto con la cantidad de comentarios.

db.createView(
    "Top 5 Genres",
    "movies",
    [
        {$lookup:{
            from:"comments",
            localField:"_id",
            foreignField:"movie_id",
            as: "comments"
        }},
        {
            $addFields: {
                comments_count: { $size: "$comments" }
            }
        },
        {$unwind:"$genres"},
        {$group:{
            _id:"$genres",
            count:{$sum:"$comments_count"}
        }},
        {$sort: {count: -1 }},
        {$limit:10}
])

db.comments.aggregate([
    {
      $lookup: {
        from: "movies",
        localField: "movie_id",
        foreignField: "_id",
        as: "movie"
      }
    },
    { $unwind: "$movie" },
    { $unwind: "$movie.genres" },   // un documento por cada género
    {
      $group: {
        _id: "$movie.genres",
        total_comments: { $sum: 1 }
      }
    },
    { $sort: { total_comments: -1 } },
    { $limit: 10 }
  ])

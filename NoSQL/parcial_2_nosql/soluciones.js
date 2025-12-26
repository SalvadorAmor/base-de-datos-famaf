use sample_airbnb

// Calcular el rating promedio por país. Listar el país, rating promedio, y cantidad de
// rating. Listar en orden descendente por rating promedio. Usar el campo
// “review_scores.review_scores_rating” para calcular el rating promedio.

db.listingsAndReviews.aggregate([
    {
        $group:{
            _id: "$address.country",
            rating_promedio: {$avg: "$review_scores.review_scores_rating"},
            cantidad_rating: {$sum: 1}
        }
    },
    {
        $sort:{
            cantidad_rating:1
        }
    }
])

// Listar los 20 alojamientos que tienen las reviews más recientes. Listar el id, nombre,
// fecha de la última review, y cantidad de reviews del alojamiento. Listar en orden
// descendente por cantidad de reviews.
// HINT: $first pueden ser de utilidad.

db.listingsAndReviews.aggregate([
    {
      $addFields:{
          reviews_quantity: { $size: "$reviews" }
      }
    },
    {
        $unwind:"$reviews"
    },
    {
        $sort: {"reviews.date": -1}
    },
    {
        $group:{
            _id:"$_id",
            name:{$first: "$name"},
            review_date:{$first: "$reviews.date"},
            review_quantity:{$first: "$reviews_quantity"}
        }
    },
    {
        $sort: {"review.date": -1}
    },
    {
        $limit: 20
    },
    {
        $sort:{"review_quantity":-1}
    }
])

// Crear la vista “top10_most_common_amenities” con información de los 10 amenities
// que aparecen con más frecuencia. El resultado debe mostrar el amenity y la
// cantidad de veces que aparece cada amenity.

db.createView(
    "top10_most_common_amenities",
    "listingsAndReviews",
    [
        {
            $unwind:"$amenities"
        },
        {
            $group:{
                _id:"$amenities",
                appear_quantity:{$sum: 1}
            }
        },
        {
            $project:{
                amenitie: "$_id",
                appear_quantity: 1,
                _id:0
            }
        },
        {
            $sort: {appear_quantity:-1}
        },
        {
            $limit:10
        }
    ]
)

//Actualizar los alojamientos de Brazil que tengan un rating global
//(“review_scores.review_scores_rating”) asignado, agregando el campo
//"quality_label" que clasifique el alojamiento como “High” (si el rating global es mayor
//o igual a 90), “Medium” (si el rating global es mayor o igual a 70), “Low” (valor por
//defecto) calidad..
//HINTS: (i) para actualizar se puede usar pipeline de agregación. (ii) El operador
//$cond o $switch pueden ser de utilidad.

db.listingsAndReviews.updateMany(
    {
        "address.country":"Brazil",
        "review_scores.review_scores_rating":{ $exists: true }
    },
    [
        {
            $set:{
                score_avg: {$avg:"$scores.score"}
            }
        },
        {
            $set:{
                quality_label: {
                    $switch:{
                        branches: [
                            {
                                case: {$gte:["$review_scores.review_scores_rating", 90]},
                                then: "High"
                            },
                            {
                                case: {$gte:["$review_scores.review_scores_rating", 70]},
                                then: "Medium"
                            }
                        ],
                        default: "Low"
                    }
                }
            }
        }
    ]
)

//(a) Especificar reglas de validación en la colección listingsAndReviews a los
//siguientes campos requeridos: name, address, amenities, review_scores, and
//reviews ( y todos sus campos anidados). Inferir los tipos y otras restricciones que
//considere adecuados para especificar las reglas a partir de los documentos de la
//colección.
//(b) Testear la regla de validación generando dos casos de fallas en la regla de
//validación y un caso de éxito en la regla de validación. Aclarar en la entrega cuales
//son los casos y por qué fallan y cuales cumplen la regla de validación. Los casos no
//deben ser triviales, es decir los ejemplos deben contener todos los campos
//especificados en la regla.

db.runCommand({
    collMod:"listingsAndReviews",
    validator:{
        $jsonSchema:{
            bsonType: "object",
            required: ["name", "address", "amenities", "review_scores", "reviews"],
            properties: {
                name:{
                    bsonType: "string",
                    description: "name is a string and is required"
                },
                address:{
                    bsonType: "object",
                    required: ["street", "suburb", "government_area",
                                "market", "country", "country_code", "location"],
                    properties:{
                        street:{
                            bsonType: "string"
                        },
                        suburb:{
                            bsonType: "string"
                        },
                        goverment_area:{
                            bsonType: "string"
                        },
                        market:{
                            bsonType: "string"
                        },
                        country:{
                            bsonType: "string"
                        },
                        country_code:{
                            bsonType: "string"
                        },
                        location:{
                            bsonType: "object"
                        }
                    }
                },
                amenities:{
                    bsonType: "array",
                    items:{
                        bsonType: "string"
                    },
                    description: "amenities is an array and is required"
                },
                review_scores:{
                    bsonType: "object",
                    required: ["review_scores_accuracy", "review_scores_cleanliness","review_scores_communication",
                                "review_scores_location", "review_scores_value", "review_scores_rating"],
                    properties:{
                        review_scores_accuracy:{
                            bsonType: "int"
                        },
                        review_scores_cleanliness:{
                            bsonType: "int"
                        },
                        review_scores_communication:{
                            bsonType: "int"
                        },
                        review_scores_location:{
                            bsonType: "int"
                        },
                        review_scores_value:{
                            bsonType: "int"
                        },
                        review_scores_rating:{
                            bsonType: "int",
                            minimum: 0,
                            maximum: 100
                        }
                    }
                },
                reviews:{
                    bsonType: "array",
                    items:{
                        bsonType: "object",
                        required: ["_id", "date", "listing_id", "reviewer_id","reviewer_name","comments" ],
                        properties: {
                            _id:{
                                bsonType: "string",
                            },
                            date:{
                                bsonType: "date"
                            },
                            listing_id:{
                                bsonType: "string"
                            },
                            reviewer_id:{
                                bsonType: "string"
                            },
                            reviewer_name:{
                                bsonType: "string"
                            },
                            comments:{
                                bsonType: "string"
                            }
                        }
                    }
                }
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error"
})

// CASOS DE PRUEBA

// Falla porque review_scores_rating supera el numero maximo (100)

db.listingsAndReviews.insertOne({
    name: "superLugar",
    address: {
        "street": "Porto, Porto, Portugal", "suburb": "",
        "government_area": "Cedofeita, Ildefonso, Sé, Miragaia, Nicolau, Vitória",
        "market": "Porto",
        "country": "Portugal",
        "country_code": "PT",
        "location": {"type": "Point", "coordinates": [-8.61308, 41.1413],
        "is_location_exact": false}
    },
    amenities: ["Wifi"],
    review_scores: {
        "review_scores_accuracy": new NumberInt("9"),
        "review_scores_cleanliness": new NumberInt("9"),
        "review_scores_checkin": new NumberInt("10"),
        "review_scores_communication": new NumberInt("10"),
        "review_scores_location": new NumberInt("10"),
        "review_scores_value": new NumberInt("9"),
        "review_scores_rating": new NumberInt("102")
    },
    reviews: [{
        "_id": "58663741",
        "date": new ISODate("2016-01-03T05:00:00.000Z"),
        "listing_id": "10006546",
        "reviewer_id": "51483096",
        "reviewer_name": "Cátia",
        "comments": "A casa da Ana e do Gonçalo foram o local escolhido para a passagem de ano com um grupo de amigos. Fomos super bem recebidos com uma grande simpatia e predisposição a ajudar com qualquer coisa que fosse necessário.\r\nA casa era ainda melhor do que parecia nas fotos, totalmente equipada, com mantas, aquecedor e tudo o que pudessemos precisar.\r\nA localização não podia ser melhor! Não há melhor do que acordar de manhã e ao virar da esquina estar a ribeira do Porto."
    }]
})

// Falla porque amenities no es un arreglo

db.listingsAndReviews.insertOne({
    name: "superLugar 2",
    address: {
        "street": "Porto, Porto, Portugal", "suburb": "",
        "government_area": "Cedofeita, Ildefonso, Sé, Miragaia, Nicolau, Vitória",
        "market": "Porto",
        "country": "Portugal",
        "country_code": "PT",
        "location": {"type": "Point", "coordinates": [-8.61308, 41.1413],
        "is_location_exact": false}
    },
    amenities: "Wifi",
    review_scores: {
        "review_scores_accuracy": new NumberInt("9"),
        "review_scores_cleanliness": new NumberInt("9"),
        "review_scores_checkin": new NumberInt("10"),
        "review_scores_communication": new NumberInt("10"),
        "review_scores_location": new NumberInt("10"),
        "review_scores_value": new NumberInt("9"),
        "review_scores_rating": new NumberInt("90")
    },
    reviews: [{
        "_id": "58663741",
        "date": new ISODate("2016-01-03T05:00:00.000Z"),
        "listing_id": "10006546",
        "reviewer_id": "51483096",
        "reviewer_name": "Cátia",
        "comments": "A casa da Ana e do Gonçalo foram o local escolhido para a passagem de ano com um grupo de amigos. Fomos super bem recebidos com uma grande simpatia e predisposição a ajudar com qualquer coisa que fosse necessário.\r\nA casa era ainda melhor do que parecia nas fotos, totalmente equipada, com mantas, aquecedor e tudo o que pudessemos precisar.\r\nA localização não podia ser melhor! Não há melhor do que acordar de manhã e ao virar da esquina estar a ribeira do Porto."
    }]
})

// Anda todo bien

db.listingsAndReviews.insertOne({
    name: "superLugarBien",
    address: {
        "street": "Porto, Porto, Portugal", "suburb": "",
        "government_area": "Cedofeita, Ildefonso, Sé, Miragaia, Nicolau, Vitória",
        "market": "Porto",
        "country": "Portugal",
        "country_code": "PT",
        "location": {"type": "Point", "coordinates": [-8.61308, 41.1413],
        "is_location_exact": false}
    },
    amenities: ["Wifi"],
    review_scores: {
        "review_scores_accuracy": new NumberInt("9"),
        "review_scores_cleanliness": new NumberInt("9"),
        "review_scores_checkin": new NumberInt("10"),
        "review_scores_communication": new NumberInt("10"),
        "review_scores_location": new NumberInt("10"),
        "review_scores_value": new NumberInt("9"),
        "review_scores_rating": new NumberInt("90")
    },
    reviews: [{
        "_id": "58663741",
        "date": new ISODate("2016-01-03T05:00:00.000Z"),
        "listing_id": "10006546",
        "reviewer_id": "51483096",
        "reviewer_name": "Cátia",
        "comments": "A casa da Ana e do Gonçalo foram o local escolhido para a passagem de ano com um grupo de amigos. Fomos super bem recebidos com uma grande simpatia e predisposição a ajudar com qualquer coisa que fosse necessário.\r\nA casa era ainda melhor do que parecia nas fotos, totalmente equipada, com mantas, aquecedor e tudo o que pudessemos precisar.\r\nA localização não podia ser melhor! Não há melhor do que acordar de manhã e ao virar da esquina estar a ribeira do Porto."
    }]
})
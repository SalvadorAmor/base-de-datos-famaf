//Buscar los documentos donde el alumno tiene:
//(i) un puntaje mayor o igual a 80 en "exam" o bien un puntaje mayor o igual a 90 en
//"quiz" y
//(ii) un puntaje mayor o igual a 60 en todos los "homework" (en otras palabras no
//tiene un puntaje menor a 60 en algún "homework")
//Las dos condiciones se tienen que cumplir juntas (es un AND)
//Se debe mostrar todos los campos excepto el _id, ordenados por el id de la clase y
//id del alumno en orden descendente y ascendente respectivamente.

db.grades.find(
    {
        scores:{
            $elemMatch:{
                $or:[
                    { type:"exam", score: {$gte:80} },
                    { type:"quiz", score: {$gte:90} }
                ],
            },
            $not:{
                $elemMatch:{ type:"homework", score: {$lt: 60} }}
            },
    },
    {_id:0},
)
.sort({class_id:-1, student_id:1})

// Calcular el puntaje mínimo, promedio, y máximo que obtuvo el alumno en las clases
// 20, 220, 420. El resultado debe mostrar además el id de la clase y el id del alumno,
// ordenados por alumno y clase en orden ascendentes

// version bien
db.grades.aggregate([
    {
        $match:{class_id:{$in:[20,220,420]}}
    },
    {
        $project:{
            student_id: 1,
            class_id: 1,
            _id: 0,
            min_score: {$min: "$scores.score"},
            max_score: {$max: "$scores.score"},
            avg_score: {$avg: "$scores.score"}
        }
    },
    {
        $sort:{
            student_id:1,
            class_id:1
        }
    }
])
// version mala pero funca
db.grades.aggregate([
    {
        $match:{
            $expr:{
                $in:["$class_id", [20,220,420]]
            }
        }
    }
])

// Para cada clase listar el puntaje máximo de las evaluaciones de tipo "exam" y el
// puntaje máximo de las evaluaciones de tipo "quiz". Listar en orden ascendente por el
// id de la clase. HINT: El operador $filter puede ser de utilidad.

db.grades.aggregate([
    {
      $project:{
          exam_score: {$filter:{
              input:"$scores",
              as: "score",
              cond: {$eq:["$$score.type","exam"]}
          }},
          quiz_score:{$filter:{
              input:"$scores",
              as: "score",
              cond: {$eq:["$$score.type","quiz"]}
          }},
          class_id: 1
      }
    },
    {
        $group:{
            _id:"$class_id",
            max_exam_score:{$max:"$exam_score.score"},
            max_quiz_score:{$max:"$quiz_score.score"}
        }
    },
    {
      $unwind:"$max_exam_score"
    },
    {
      $unwind:"$max_quiz_score"
    },
    {
        $project:{
            class_id: "$_id",
            max_exam_score: 1,
            max_quiz_score: 1,
            _id:0
        }
    },
    {
        $sort:{
            class_id: 1,
        }
    }
])

// Crear una vista "top10students" que liste los 10 estudiantes con los mejores
// promedios

db.createView(
    "top10students",
    "grades",
    [
        {
          $unwind: "$scores"
        },
        {
            $group:{
                _id:"$student_id",
                promedio:{$avg:"$scores.score"}
            }
        },
        {
            $sort:{
                promedio:-1
            }
        },
        {
            $limit: 10
        }
    ]
)

db.createView(
    "top10students",
    "grades",
    [
        {
            $group: {
                _id: "$student_id",
                avg_score_per_class: {
                    $addToSet: {
                        class: "$class_id",
                        avg_score: { $avg: "$scores.score" }
                    }
                }
            }
        },
        {
            $project: {
                _id: 0,
                student: "$_id",
                avg_score: {
                    $avg: "$avg_score_per_class.avg_score"
                }
            }
        },
        { $sort: { avg_score: -1 } },
        { $limit: 10 },
    ]
)

// Actualizar los documentos de la clase 339, agregando dos nuevos campos: el
// campo "score_avg" que almacena el puntaje promedio y el campo "letter" que tiene
// el valor "NA" si el puntaje promedio está entre [0, 60), el valor "A" si el puntaje
// promedio está entre [60, 80) y el valor "P" si el puntaje promedio está entre [80, 100].
// HINTS: (i) para actualizar se puede usar pipeline de agregación. (ii) El operador
// $cond o $switch pueden ser de utilidad


db.grades.updateMany(
    {class_id:339},
    [
        {
            $set:{
                score_avg: {$avg:"$scores.score"}
            }
        },
        {
            $set:{
                letter: {
                    $switch:{
                        branches: [
                            {
                                case: {$and: [ {$gte:["$score_avg", 60]} ,{$lt:["$score_avg",80]} ] },
                                then: "A"
                            },
                            {
                                case: {$and: [ {$gte:["$score_avg", 80]} ,{$lte:["$score_avg", 100]} ]},
                                then: "P"
                            }
                        ],
                        default: "NA"
                    }
                }
            }
        }
    ]
)




db.runCommand({
    collMod: "grades",
    validator: {
        $jsonSchema:{
            bsonType: "object",
            required: ["class_id", "student_id"],
            properties: {
                class_id:{
                    bsonType: "int",
                    description: "class must be an int and is required"
                    },
                student_id:{
                    bsonType: "int",
                    description: "student must be an int and is required"
                    },
                letter:{
                    enum: ["A", "P", "NA"],
                    description: "bobo"
                    },
                scores:{
                    bsonType: "array",
                    items: {
                        bsonType: "object",
                        required: ["type", "score"]
                        }
                    }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "error"
})

db.grades.insertOne({
    student_id: 1000,
    class_id: "hola",
    scores: [{"type":"exam", "score":70}]
})
/Buscar las ventas realizadas en "London", "Austin" o "San Diego"; a un customer con
//edad mayor-igual a 18 años que tengan productos que hayan salido al menos 1000
//y estén etiquetados (tags) como de tipo "school" o "kids" (pueden tener más
//etiquetas).
//Mostrar el id de la venta con el nombre "sale", la fecha (“saleDate"), el storeLocation,
//y el "email del cliente. No mostrar resultados anidados.

db.collection.aggregate([
  // Etapa 1: Filtrar por ubicación (storeLocation) y edad (customer.age)
  {
    $match: {
      storeLocation: { $in: ["London", "Austin", "San Diego"] },
      "customer.age": { $gte: 18 }
    }
  },

  // Etapa 2: Crear un campo temporal que contenga solo los ítems que cumplen
  // las condiciones de "tags" Y (precio * cantidad >= 1000)
  {
    $addFields: {
      "qualifyingItems": {
        $filter: {
          input: "$items",
          as: "item",
          cond: {
            $and: [
              // Condición 1: El total del ítem (precio * cantidad) es >= 1000
              {
                $gte: [
                  { $multiply: ["$$item.price", "$$item.quantity"] },
                  1000
                ]
              },
              // Condición 2: El array 'tags' contiene "school" O "kids"
              {
                $gt: [
                  { $size: { $setIntersection: [ "$$item.tags", ["school", "kids"] ] } },
                  0
                ]
              }
            ]
          }
        }
      }
    }
  },

  // Etapa 3: Filtrar de nuevo para quedarnos solo con las ventas que
  // tuvieron al menos un ítem que calificó (el array no está vacío)
  {
    $match: {
      "qualifyingItems.0": { $exists: true }
    }
  },

  // Etapa 4: Proyectar (formatear) la salida final
  {
    $project: {
      _id: 0, // Ocultar el _id original
      sale: "$_id", // Renombrar _id a "sale"
      saleDate: "$saleDate",
      storeLocation: "$storeLocation",
      email: "$customer.email" // Acceder al campo anidado
    }
  }
])

db.sales.aggregate([
  { $match: {
      storeLocation: { $in: ["London", "Austin", "San Diego"] },
      "customer.age": { $gte: 18 }
  }},

  { $unwind: "$items" },

  { $set: {
      lineTotal: { $multiply: ["$items.price", "$items.quantity"] }
  }},
  // 4) Filtro por línea: subtotal ≥ 1000 y tags contiene school o kids
  { $match: {
      lineTotal: { $gte: 1000 },
      "items.tags": { $in: ["school", "kids"] }
  }},
  // 5) Subir campos planos y deduplicar por venta
  { $project: {
      _id: 0,
      sale: "$_id",
      saleDate: 1,
      storeLocation: 1,
      email: "$customer.email"
  }},
  // 6) Por si una venta cumple por más de un ítem, agrupo para no repetir
  { $group: {
      _id: "$sale",
      saleDate: { $first: "$saleDate" },
      storeLocation: { $first: "$storeLocation" },
      email: { $first: "$email" }
  }},
  // 7) Formato final (sin anidar)
  { $project: {
      _id: 0,
      sale: "$_id",
      saleDate: 1,
      storeLocation: 1,
      email: 1
  }}
]);


db.sales.find(
    {
        storeLocation:{$in:["London","Austin","San Diego"]},
        "customer.age":{$gte:18},
        items:{
            $elemMatch:{
                price:{$gte:1000},
                tags:{
                    $in:["school","kids"]
                }
            }
        }
    },
    {_id:0,sale:"$_id",saleDate:1,storeLocation:1,email:"$customer.email"}
)

//Buscar las ventas de las tiendas localizadas en Seattle, donde el método de compra
//sea ‘In store’ o ‘Phone’ y se hayan realizado entre 1 de febrero de 2014 y 31 de enero
//de 2015 (ambas fechas inclusive). Listar el email y la satisfacción del cliente, y el
//monto total facturado, donde el monto de cada item se calcula como 'price *
//quantity'. Mostrar el resultado ordenados por satisfacción (descendente), frente a
//empate de satisfacción ordenar por email (alfabético).

db.sales.aggregate([
    {
        $match:{
            storeLocation:"Seattle",
            purchaseMethod:{$in:['In store','Phone']},
            saleDate: {
                $gte: new Date("2014-02-01T00:00:00Z"),
                $lt:  new Date("2015-02-01T00:00:00Z")
            }
        },
    },
    {
        $addFields:{
            total:
                {$sum:
                    {
                        $map:{
                            input:"$items",
                            as:"item",
                            in:{$multiply:["$$item.price","$$item.quantity"]}
                        }
                    }
                }
        }
    },
    {
        $project:{
            email:"$customer.email",
            satisfaction:"$customer.satisfaction",
            total:1
        }
    },
    {
        $sort:{total:-1}
    }

])


db.sales.aggregate([
    {
        $match:{
            storeLocation:"Seattle",
            purchaseMethod:{$in:['In store','Phone']},
        },
    },
    {
        $addFields:{
            total:{
                $map:{
                    input:"$items",
                    as:"item",
                    in:{$multiply:["$$item.price","$$item.quantity"]}
                }
            }
        }
    },
    {
        $project:{
            email:"$customer.email",
            satisfaction:"$customer.satisfaction",
            total_price:{
                $reduce:{
                    input:"$total",
                    initialValue:0,
                    in:{$add:["$$value","$$this"]}
                }
            }
        }
    },
    {
        $sort:{total_price:-1}
    }

])

db.sales.aggregate([
    {
        $match:{
            storeLocation:"Seattle",
            purchaseMethod:{$in:['In store','Phone']},
        },
    },
    {
        $addFields:{
            total:{
                $map:{
                    input:"$items",
                    as:"item",
                    in:{$multiply:["$$item.price","$$item.quantity"]}
                }
            }
        }
    },
    {
      $unwind:"$total"
    },
    {
        $group:{
            _id:"$_id",
            email:{$first:"$customer.email"},
            satisfaction:{$first:"$customer.satisfaction"},
            total_price:{$sum:"$total"}
        }
    },
    {
        $sort:{total_price:-1}
    }
])

//Crear la vista salesInvoiced que calcula el monto mínimo, monto máximo, monto
//total y monto promedio facturado por año y mes. Mostrar el resultado en orden
//cronológico. No se debe mostrar campos anidados en el resultado.

db.createView(
    "salesInvoiced",
    "sales",
    [
        {
            $addFields:{
                total:
                    {$sum:
                        {
                            $map:{
                                input:"$items",
                                as:"item",
                                in:{$multiply:["$$item.price","$$item.quantity"]}
                            }
                        }
                    }
            }
        },
        {
            $group:{
                _id:{
                    year:{$year:"$saleDate"},
                    month:{$month:"$saleDate"}
                },
                max_sale:{$max:"$total"},
                min_sale:{$min:"$total"},
                avg_sale:{$avg:"$total"},
                total_sale:{$sum:"$total"}
            }
        },
        {
            $project: {
            _id: 0,
            year:  "$_id.year",
            month: "$_id.month",
            max_sale: 1,
            min_sale: 1,
            avg_sale: 1,
            total_sale: 1
        }
        },
            {
                $sort:{year:1,month:1}
            }
        ]
)

//Mostrar el storeLocation, la venta promedio de ese local, el objetivo a cumplir de
//ventas (dentro de la colección storeObjectives) y la diferencia entre el promedio y el
//objetivo de todos los locales.

db.storeObjectives.aggregate([
    {
        $lookup:{
            from:"sales",
            localField:"_id",
            foreignField:"storeLocation",
            as:"sales"
        }
    },
    {
        $project:{
            _id:0,
            location:"$_id",
            objective_sales:"$objective",
            real_sales:{$size:"$sales"},
            diff:{
                $subtract:[{$size:"$sales"},"$objective"]
            }
        }
    }
])

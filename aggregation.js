// Agregación MongoDB
// Pipe con una serie de etapas que se ejecutan en memoria

// Sintaxis
// db.<coleccion>.aggregate(
    // [
    //     {etapa1}, // utiliza operadores de agregación
    //     {etapa2},
    //     ...
    // ],
    // {documento de opciones}
// )

// Etapa
// {$operadorEtapa: {$operador, $operador,...}}

// Operador $project

db.titulos.aggregate([
    {$project: {_id: 0, titulo: 1, categorias: 1}}
])

// Cada etapa actúa sobre la anterior
// Field reference utiliza $<nombre-del-campo>

db.titulos.aggregate([
    {$project: {_id: 0, titulo: 1, categorias: 1}},
    {$project: {title: "$titulo"}} // expresión
])

// $project también permite expresiones

// set de datos

db.libros.insert([
    {titulo: "Cien Años de Soledad", autor: "Gabriel García Márquez", stock: 10},
    {titulo: "La Ciudad y los Perros", autor: "Mario Vargas Llosa", stock: 10, prestados: 2},
    {titulo: "El Otoño del Patriarca", autor: "Gabriel García Márquez", stock: 10, prestados: 0},
])

db.libros.aggregate([
    {$project: {
        titulo: 1,
        _id: 0,
        prestados: {
            $cond: {
                if: {$eq: [0, "$prestados"]},
                then: "$$REMOVE",
                else: "$prestados"
            }
        }
    }}
])

// $sort ordenar el set documentos de la etapa anterior

// set de datos

use gimnasio2

db.clientes.insert([
    {nombre: "Juan", apellidos: "Pérez", alta: new Date(2020,8,5), actividades: ["padel","tenis","esgrima"]},
    {nombre: "Luisa", apellidos: "López", alta: new Date(2020,9,15), actividades: ["aquagym","tenis","step"]},
    {nombre: "Carlos", apellidos: "Pérez", alta: new Date(2020,10,8), actividades: ["aquagym","padel","cardio"]},
    {nombre: "José", apellidos: "Gómez", alta: new Date(2020,8,25), actividades: ["pesas","cardio","step"]},
])

db.clientes.aggregate([
    {$project: {cliente: {$toUpper: "$apellidos"}, _id: 0}},
    {$sort: {cliente: 1}}
])

db.clientes.aggregate([
    {$project: {nombre: 1, apellidos: 1, _id: 0, mesAlta: {$month: "$alta"}}},
    {$sort: {mesAlta: -1, apellidos: 1}}
])

// $group

// { $group: {
//    _id: <expresión>, Agrupa por el campo _id
//    <campo>: {<acumulador>: <expresión>},
//    <campo>...
// }}

db.clientes.aggregate([
    {$project: {mesAlta: {$month: "$alta"}, _id: 0}},
    {$group: {_id: "$mesAlta", numeroAltasMes: {$sum: 1}}}, // Necesitamos _id para agrupar
    {$project: {mes: "$_id", numeroAltasMes: 1, _id: 0}}, // pero podemos volcar sus valores en otro campo nuevo y eliminarlo
    {$sort: {numeroAltasMes: -1}}
])

// Sobre cualquier operador pero especialmente con $group cada etapa tiene, por defecto, un límite de 100 megabytes de RAM
// se puede sobrepasar con la opción allowDiskUse como true

db.clientes.aggregate([
    {$project: {mesAlta: {$month: "$alta"}, _id: 0}},
    {$group: {_id: "$mesAlta", numeroAltasMes: {$sum: 1}}}, // Necesitamos _id para agrupar
    {$project: {mes: "$_id", numeroAltasMes: 1, _id: 0}}, // pero podemos volcar sus valores en otro campo nuevo y eliminarlo
    {$sort: {numeroAltasMes: -1}}
], {allowDiskUse: true})

// Otros ejemplos con otros operadores

use shop2

db.pedidos.insert([
    {sku: "v101", cantidad: 12, precio: 20, fecha: ISODate("2020-11-21")},
    {sku: "v101", cantidad: 6, precio: 20, fecha: ISODate("2020-11-22")},
    {sku: "v101", cantidad: 4, precio: 20, fecha: ISODate("2020-11-21")},
    {sku: "v102", cantidad: 7, precio: 10.3, fecha: ISODate("2020-11-21")},
    {sku: "v102", cantidad: 5, precio: 10.9, fecha: ISODate("2020-11-21")}
])

// Total ventas por día de la semana

db.pedidos.aggregate([
    {$group: {_id: {$dayOfWeek: "$fecha"}, totalVentas: {$sum: {$multiply: ["$cantidad","$precio"]}}}},
    {$project: {diaSemana: "$_id", totalVentas: 1, _id: 0}},
    {$sort: {diaSemana: -1}}
])

// Promedio de cantidad de producto 

db.pedidos.aggregate([
    {$group: {_id: "$sku", cantidadPromedioPedido: {$avg: "$cantidad"}}},
    {$project: {skuProducto: "$_id", cantidadPromedioPedido: 1, _id: 0}},
])









// $match


// En niveles inferiores

db.clientes.insert(
    {
        nombre: "Juan", 
        apellidos: "Pérez", 
        alta: new Date(2020,9,5), 
        actividades:[
            {nombre: "padel", activo: true, turno: 'mañana'},
            {nombre: "esgrima", activo: false, turno: 'mañana'},
            {nombre: "tenis", activo: true, turno: 'tarde'}
        ]
    }
)
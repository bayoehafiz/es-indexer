var elasticsearch = require('elasticsearch');
var mysql = require('mysql');

var client = new elasticsearch.Client({
    host: 'http://localhost:9200',
    // log: 'trace', // <- verbose method (DEVELOPMENT ONLY!)
});

var createIndex = function(name) {
    client.indices.create({
        index: name
    }, function(err, resp, status) {
        if (err) {
            console.log(err);
        } else {
            console.log("create", resp);
        }
    });
}

var dataKeywordFormat = function(res) {
    return data = {
        "index": "keyword",
        "type": "_doc",
        "id": res.id,
        "body": {
            "keyword": res.suggestion,
            "area": res.area,
            "latitude": (res.latitude == null) ? '' : res.latitude,
            "longitude": (res.longitude == null) ? '' : res.longitude,
            "administrative_type": (res.administrative_type == null) ? '' : res.administrative_type,
            "place_id": (res.place_id == null) ? '' : res.place_id
        }
    }
}

var dataRoomFormat = function(res) {
    // compile tags
    var tags = [];
    var source = (res.tags !== undefined) ? res.tags : [];
    if (source.length > 0) {
        source.forEach(function(t) {
            tags.push(t.id);
        })
    }

    // compile apartment_projects
    var apartmentProject = null;
    if (res.apartment_project !== undefined) {
        var apartmentProject = {
            "name": res.apartment_project.name,
            "project_code": res.apartment_project.project_code,
            "slug": res.apartment_project.slug,
            "address": res.apartment_project.address
        };
    }

    // Manipulate 'completion' fields value
    var name = (res.name == null) ? '' : res.name;
    var address = (res.address == null) ? '' : res.address;
    var area_city = (res.area_city == null) ? '' : res.area_city;
    var area_subdistrict = (res.area_subdistrict == null) ? '' : res.area_subdistrict;
    var area_big = (res.area_big == null) ? '' : res.area_big;

    return data = {
        "index": "room",
        "type": "_doc",
        "id": res.id,
        "body": {
            "code": res.code,
            "song_id": res.song_id,
            "name": name,
            "address": address,
            "area_subdistrict": area_subdistrict,
            "area_city": area_city,
            "area_big": area_big,
            "location_id": res.location_id,
            "latitude": res.latitude,
            "longitude": res.longitude,
            "gender": res.gender,
            "status": res.status,
            "room_available": res.room_available,
            "room_count": res.room_count,
            "is_promoted": (res.is_promoted) ? true : false,
            "slug": res.slug,
            "is_booking": (res.is_booking == 1) ? true : false,
            "price_daily": (res.price_daily != null) ? res.price_daily : 0,
            "price_weekly": (res.price_weekly != null) ? res.price_weekly : 0,
            "price_monthly": (res.price_monthly != null) ? res.price_monthly : 0,
            "price_yearly": (res.price_yearly != null) ? res.price_yearly : 0,
            "price_remark": res.price_remark,
            "fac_room_other": res.fac_bath_other,
            "fac_bath_other": res.fac_bath_other,
            "fac_share_other": res.fac_share_other,
            "fac_near_other": res.fac_near_other,
            "description": res.description,
            "size": res.size,
            "is_verified_address": (res.is_verified_address == 1) ? true : false,
            "is_verified_phone": (res.is_verified_phone == 1) ? true : false,
            "is_verified_kost": (res.is_verified_kost == 1) ? true : false,
            "is_visited_kost": (res.is_verified_kost == 1) ? true : false,
            "floor": res.floor,
            "furnished": (res.furnished == 1) ? true : false,
            "view_count": res.view_count,
            "unit_type": res.unit_type,
            "photo_count": res.photo_count,
            "unit_number": res.unit_number,
            "youtube_id": res.youtube_id,
            "promotion": (res.promotion) ? true : false,
            "kost_updated_date": res.kost_updated_date,
            "created_at": res.created_at,
            "updated_at": res.updated_at,
            // "tags": tags,
            // "apartment_project": res.apartment_project
        }
    }
}

var startIndexing = function(index_name) {
    require('dotenv').config();
    var pool = mysql.createPool({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_DATABASE
    });

    if (index_name == 'room') var table = process.env.ROOM_TABLE;
    else var table = process.env.KEYWORD_TABLE;

    var countQuery = "SELECT count(*) as total FROM " + table;
    var chunkSize = process.env.CHUNK_SIZE;

    pool.getConnection(function(err, connection) {
        if (err) {
            connection.release();
            console.log("Error on getConnection:", err);
            return;
        }

        connection.query(countQuery, {}, function(err, result) {
            if (err) {
                connection.release();
                console.log("Error on getConnection:", err);
                return;
            }

            if (result && result[0]) {
                var totalRows = result[0]['total'];
                console.log("Total rows in DB:", totalRows);
                var periods = Math.ceil(totalRows / chunkSize)
                console.log("Total chunks:", periods);

                var selectQuery = "SELECT * FROM " + table + " ORDER BY id DESC LIMIT ";
                var counter = 1;

                for (var i = 0; i < periods; i++) {
                    var offset = i * chunkSize;
                    var runQuery = selectQuery + offset + "," + chunkSize;

                    connection.query(runQuery, {}, function(err, results) {
                        if (err) {
                            console.log("Error on runQuery:", err);
                            return;
                        }

                        var stopped = false;
                        for (var j = 0; j < results.length; j++) {
                            if (!stopped) {
                                if (index_name == 'room') {
                                    client.index(dataRoomFormat(results[j]), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                } else {
                                    client.index(dataKeywordFormat(results[j]), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            }
                        }
                    });

                    counter++;
                }

                connection.release();
            }

        });
    });

    // return indexedData;
}

// Main executions
console.log("Type 'keyword' or 'room' to start indexing.");
var stdin = process.openStdin();
stdin.addListener("data", function(d) {
    var type = d.toString().trim();
    if (type == 'keyword' || type == 'room') {
        console.log("Begin indexing: " + type + "...");
        // createIndex('test');
        startIndexing(type);
    } else {
        console.log("Type 'keyword' or 'room' to start indexing.");
    }
});

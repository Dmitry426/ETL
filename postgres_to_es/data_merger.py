from  validation_classes import FilmWork

class Data_Merger:
    def __init__(self):
        self. desired_structure = {
            "fw_id": "",
            "imdb_rating": "",
            "genre": [],
            "title": "",
            "description": "",
            "director": "",
            "actors_names": [],
            "writers_names": [],
            "writers": [],
            "actors": [],
        }
    def  combine_tables(self,obj:dict):
        self.desired_structure ["fw_id"] = obj['film_id']
        self.desired_structure ['imdb_rating'] = obj['rating']
        self.desired_structure ['title'] = obj['title']
        self.desired_structure ["description"] = obj['description']
        if obj["role"] == "director" and obj["full_name"] != self.desired_structure ["director"]:
            self.desired_structure ['director'] = obj['full_name']
        if obj["role"] == "actor" and obj["full_name"] not in self.desired_structure ["actors_names"]:
            self.desired_structure ["actors_names"].append(obj["full_name"]),
            self.desired_structure ["actors"].append({"id": obj['person_id'],
                                                "name": obj["full_name"]})
        if obj["role"] == "writer" and obj["full_name"] not in self.desired_structure ["writers_names"]:
            self.desired_structure ["writers_names"].append(obj["full_name"]),
            self.desired_structure ["writers"].append({"id": obj['person_id'],
                                                 "name": obj["full_name"]})

    def validate_and_return(self):
        validated_result = FilmWork.parse_obj(self.desired_structure)
        return  validated_result
    def clean(self):
        self.desired_structure = {
            "fw_id": "",
            "imdb_rating": "",
            "genre": [],
            "title": "",
            "description": "",
            "director": "",
            "actors_names": [],
            "writers_names": [],
            "writers": [],
            "actors": [],
        }
from validation_classes import FilmWork ,Datetime_serialization


class Data_Merger:
    def __init__(self):
        self.desired_structure = {
            "fw_id": "", "imdb_rating": "",
            "genre": [], "title": "",
            "description": "", "director": "",
            "actors_names": [], "writers_names": [],
            "writers": [], "actors": [],
        }

    def combine_tables(self, obj: dict):
        """Merges multiple tables into one dict  """
        self.desired_structure['fw_id'] = obj['film_id']
        self.desired_structure['imdb_rating'] = obj['rating']
        self.desired_structure['title'] = obj['title']
        self.desired_structure["description"] = obj['description']
        self.merging_conditions(obj=obj)

    def validate_and_return(self):
        """Validates data and returns dataclass obj  """
        validated_result = FilmWork.parse_obj(self.desired_structure)
        self.empty_dataset()
        return validated_result
    @staticmethod
    def last_updated(updated_at:str):
        updated_at = Datetime_serialization.parse_obj({"updated_at":updated_at})
        return updated_at

    def merging_conditions(self, obj: dict):
        """Merging conditions for actors, directors and writes fields """
        if obj['role'] == "director" and obj["full_name"] != self.desired_structure["director"]:
            self.desired_structure['director'] = obj['full_name']
        if obj["role"] == "actor" and obj["full_name"] not in self.desired_structure["actors_names"]:
            self.desired_structure["actors_names"].append(obj["full_name"]),
            self.desired_structure["actors"].append({"id": obj['person_id'],
                                                     "name": obj["full_name"]})
        if obj["role"] == "writer" and obj["full_name"] not in self.desired_structure["writers_names"]:
            self.desired_structure["writers_names"].append(obj["full_name"]),
            self.desired_structure["writers"].append({"id": obj['person_id'],
                                                      "name": obj["full_name"]})

    def empty_dataset(self):
        """ Method to empty merged dict"""
        self.desired_structure = {
            "fw_id": "", "imdb_rating": "",
            "genre": [], "title": "",
            "description": "", "director": "",
            "actors_names": [], "writers_names": [],
            "writers": [], "actors": [],
        }

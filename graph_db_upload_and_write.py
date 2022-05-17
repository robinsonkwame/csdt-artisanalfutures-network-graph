
from neo4j import GraphDatabase
import pandas as pd
import wn
import hashlib
import streamlit as st 
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode

import os
from dotenv import load_dotenv
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# SHOULD BE IN .env
NEO4J_CONFIG = {
    'uri': os.environ.get('CSDT_GRAPH_URI'),
    'database':'neo4j',
    'auth': {'user': os.environ.get('CSDT_USER'), 'pwd': os.environ.get('CSDT_PASSWORD')}
}

def get_neo4j_driver(config=NEO4J_CONFIG):
    return GraphDatabase.driver(
        config['uri'],
        auth=(config['auth']['user'], config['auth']['pwd'])
    )

def generate_hash(of_this_string, encoding='utf-8'):
    encoded_string = of_this_string.encode(encoding)
    return hashlib.sha224(encoded_string).hexdigest()

def setup_everything():
    driver = get_neo4j_driver()
    print(driver.verify_connectivity())
    with driver.session() as session:
        print("\t... ")
        session.write_transaction(
            load_and_setup_wordnet
        )
        print("\t... finished")

class App:
    def __init__(self):
        self.driver = get_neo4j_driver()

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def add_offering(self, maker=None, craft_id=None,
                     url=None, product_name=None):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._add_offering, maker, craft_id, url, product_name)
            for record in result:
                print("Created artisan: {p1}".format(
                    p1=record['p1']))

    def add_ppm_relationships_of_type(self, craft_id=None,
                                       ppm_node_name=None, # .e.g women owned
                                       ppm_craft_id_relationship="principle",
                                       uri_set=None,
                                       uri_relationship_type="wn__mero_part"):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._add_ppm_relationships_of_type, craft_id, ppm_node_name,
                ppm_craft_id_relationship, uri_set, uri_relationship_type)


    # Step 1: We add nodes related to an artisan offering and connect them
    @staticmethod
    def _add_offering(tx=None,
                      maker=None,
                      craft_id=None,
                      url=None,
                      product_name=None):
        cypher = (
            "MERGE (a:Artisan {name: $maker }) "
            "MERGE (cid:CraftID {name: $craft_id }) "
            "MERGE (url:Url {name: $url }) "
            "MERGE (prod:Product {name: $product_name }) "
            "MERGE (a)-[:MADE]->(cid) "
            "MERGE (cid)-[:HAS_URL]->(url) "
            "MERGE (cid)-[:INSTANCE_OF]->(prod) "
        )
        result = tx.run(cypher, maker=maker, craft_id=craft_id, url=url, product_name=product_name)
        try:
            return [record
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=cypher, exception=exception))

    # Step 2: We add princples, materials and processes relationships related to 
    # the offering by referencing into wordnet
    @staticmethod
    def _add_ppm_relationships_of_type(tx=None,
                                       craft_id=None,
                                       ppm_node_name=None, # .e.g women owned
                                       ppm_craft_id_relationship="principle",
                                       uri_set=None,
                                       uri_relationship_type="wn__mero_part"):
        if craft_id is not None:
            ppm_craft_id_relationship = ppm_craft_id_relationship.title()
            # we use f strings because cypher can't handle parameterized labels
            # see: https://community.neo4j.com/t/dynamically-filling-in-label-in-a-query-gives-an-error/50631/2
            
            # So for AuraDB we require that (u) were added in a prior call
            cypher = (
                "MATCH (u:Resource) WHERE ANY (uri IN u.uri WHERE uri IN $uri_set) "
                f"MERGE (ppm:{ppm_craft_id_relationship} {{name: $ppm_node_name}}) "
                "MERGE (cid:CraftID {name: $craft_id} )"
                f"MERGE (u)-[:{uri_relationship_type}]->(ppm)"
                f"MERGE (ppm)-[p:{ppm_craft_id_relationship}]->(cid) "
            )            
            result = tx.run(
                cypher, craft_id=craft_id, ppm_node_name=ppm_node_name, 
                ppm_craft_id_relationship=ppm_craft_id_relationship.title(),uri_set=uri_set, uri_relationship_type=uri_relationship_type)


    def add_uris(self, uri_set=None):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._add_uris, uri_set)

    @staticmethod
    def _add_uris(tx=None,uri_set=None):
            #note: this appears to only write one uri_set?
            cypher = (
                "WITH $uri_set AS uri_set "
                "UNWIND uri_set AS uri "
                "MERGE (:Resource {uri: uri}) "
            )
            result = tx.run(
                cypher, uri_set=uri_set 
            )

    @staticmethod
    def _locally_map_phrase_to_uris(the_full_phrase, kind_of_phrase="principle"):
        """
        A given phrase that describes a material, process or principle
        can be expressed as a composition of wordnet labels and relationships.
        
        For simplicity we stem the phrase and create it as a new node formed from 
        wordnet relationships. The exact relationship is a theoretical question,
            https://globalwordnet.github.io/gwadoc/#mero_substance;
            https://globalwordnet.github.io/gwadoc/#mero_part
        
        Could both fit but to know which we'd have to know more about the phrase.
        We would have to know if removing one of the words would ruin the phrase.
        For example: "blue bead," has blue dye as a mero_part but "frosted cake,"
        has frosting as a mero_substance because if you remove the frosting some
        would argue (vehmently) that you do not have a cake. The removal parts causes
        different effects depending on the consitutive relationship invovled.

        To keep things moving we chose mero_part since most artisan crafts components materially fall into that bucket, although conceptually or intellectually
        they are more substantivie (or like a substance)
        """
        print(the_full_phrase + ' ' + kind_of_phrase)

        en = wn.Wordnet('oewn:2021')

        PREFIX = "https://en-word.net/id/"
        phrases_and_words_to_look_up =\
            [the_full_phrase] + the_full_phrase.split()
        this_pos_lookup_order = ['n','v','a']

        the_uris = []
        for word in phrases_and_words_to_look_up:
            if kind_of_phrase == 'principle':
                this_pos_lookup_order = ['a', 'v', 'n']# did ['n', 'v', 'a']?
            # assert isinstance(list, this_pos_lookup_order)
            for pos in this_pos_lookup_order:
                a_potential_canonical_word = wn.synsets(word, pos=pos)
                if a_potential_canonical_word:
                    the_uris.append(
                        PREFIX+a_potential_canonical_word[0].id
                    )
                    break
        return the_uris

    def add_factory_made_relationships(self, the_phrases=None, craft_id=None,
                                        ppm_relationship=None, factory_made_relationship="IS_FACTORY_MADE"):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._add_factory_made_relationships, the_phrases, craft_id, ppm_relationship, factory_made_relationship)

    @staticmethod
    def _add_factory_made_relationships(tx=None, the_phrases=None, craft_id=None,
                                        ppm_relationship="Materials", factory_made_relationship="IS_FACTORY_MADE"):
        if the_phrases is not None:
            cypher = (
                f"MATCH (m:{ppm_relationship}) WHERE ANY (name IN m.name WHERE name IN $the_phrases) "
                "MERGE (cid:CraftID {name: $craft_id} ) "
                f"MERGE (m)-[:{factory_made_relationship}]->(cid) "
            )
            result = tx.run(
                cypher, craft_id=craft_id, the_phrases=the_phrases,
                factory_made_relationship=factory_made_relationship)

    def clear_database(self):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._clear_database)

    @staticmethod
    def _clear_database(tx=None):
            cypher = (
                "MATCH (n) DETACH DELETE n"
            )
            result = tx.run(cypher)


def get_grid_options(data):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=True, resizable=True, filterable=True, sorteable=True)
    #gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
    gb.configure_side_bar() #Add a sidebar
    #gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
    gridOptions = gb.build()
    return gridOptions

def debug_write_to_graph_db(df):
    print("CALLED DEBUG TO GRAPH",df)

def stage_database_write(df):
    app = App()
    app.clear_database()
    app.close()

    write_to_graph_db(df)

def write_to_graph_db(df):
    app = App() # should we use with..?
    for THE_ROW_INDEX in range(len(df)):
        hash_of_offering = generate_hash(
            df.loc[THE_ROW_INDEX].artisan+df.loc[THE_ROW_INDEX]['product name']
        )

        # Add the product/offering
        app.add_offering(
            df.loc[THE_ROW_INDEX].artisan,
            craft_id=hash_of_offering,
            url=df.loc[THE_ROW_INDEX].url,
            product_name=df.loc[THE_ROW_INDEX]['product name']
        )
        # Add process, materials and principle relationships
        principle_process_materials = ['principles', 'processes', 'materials']
        for bucket_name in principle_process_materials:
            print(bucket_name)
            the_bucket = df.loc[THE_ROW_INDEX][bucket_name].split(',')
            for item_in in the_bucket:
                kind_of_phrase = bucket_name #bucket_name[:-1]
                if item_in:
                    uris = app._locally_map_phrase_to_uris(
                        item_in,
                        kind_of_phrase
                    )
                    print(item_in, uris, "< those uris", len(uris))
                    if len(uris) > 0:
                        item_in = item_in.strip() # really need an interface
                        # For AuraDB we did not load Wordnet (exceeds 175k nodes)
                        # So we only add those URIs that we need
                        app.add_uris(uri_set=uris)

                        app.add_ppm_relationships_of_type(
                            craft_id=hash_of_offering, uri_set=uris,
                            ppm_node_name=item_in,
                            ppm_craft_id_relationship=bucket_name
                    )

            # Add the relationships for industrial / factory made materials
            principle_process_materials = ['principles', 'processes', 'materials']
            factory_made_column = "industrial scale items"
            #link_these_items = set(df.loc[THE_ROW_INDEX][factory_made_column].split(','))
            factory_made_item_phrase = df.loc[THE_ROW_INDEX][factory_made_column]
            link_these_items = set()
            if pd.notna(factory_made_item_phrase):
                link_these_items = set(factory_made_item_phrase.split(','))

                for bucket_name in principle_process_materials:
                    the_bucket = set(df.loc[THE_ROW_INDEX][bucket_name].split(','))
                    add_factory_made_to_these = [
                        item.strip() for item in link_these_items.intersection(the_bucket)
                    ] # argh we really need an interface!

                    print(add_factory_made_to_these, bucket_name)
                    for item_in in the_bucket:
                        kind_of_phrase = bucket_name #bucket_name[:-1]
                        app.add_factory_made_relationships(
                            the_phrases=add_factory_made_to_these,
                            craft_id=hash_of_offering,
                            ppm_relationship=bucket_name.title(), # should have interface
                            factory_made_relationship="IS_FACTORY_MADE"
                        )
    app.close()

NEO4J_BROWSER_CYPHER = "MATCH (n:CraftID)-[r]-(b) RETURN n,r,b LIMIT 2000"

if __name__ == "__main__":
    uploaded_file = st.file_uploader("Please export and upload a .csv file from the Artisanal Futures Database here")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        grid_return1 =\
            AgGrid(
                df,
                get_grid_options(df),
                enable_enterprise_modules=True,
            )

        # if st.button('[DEBUG] Print dataframe to console'):
        #     df_to_write = grid_return1['data']
        #     print(df_to_write)s

        if st.button('Write your database Graph database',
                     on_click=stage_database_write,
                     args=(grid_return1['data'], )):
            st.write("1. Go to the [Artisanal Futures graph database](https://ba50d8ea.databases.neo4j.io/browser/)")
            st.write("2. Log in with `neo4j`/`csdt`")
            st.write(f"3. Go to the shell UI, enter `{NEO4J_BROWSER_CYPHER}` and press play. You can now zoom in, expand and drag the results around.")
            st.write("4. (optional) You can also visualize the graph using [Bloom here](https://bloom.neo4j.io/index.html?connectURL=neo4j%2Bs%3A%2F%2Fba50d8ea.databases.neo4j.io)")

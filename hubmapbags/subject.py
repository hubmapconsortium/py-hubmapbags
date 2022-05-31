import pandas as pd
import os

def __get_subject_granularity(donor):
	return 'cfde_subject_granularity:0'

def __get_subject_ethnicity(donor):
    for datum in donor:
        if datum['grouping_concept_preferred_term'] == 'Race':
            return datum['preferred_term']

def __get_subject_sex(donor):
    for datum in donor:
        if datum['grouping_concept_preferred_term'] == 'Sex':
            return datum['preferred_term']

def __get_subject_age_at_enrollment(donor):
    for datum in donor:
        if datum['grouping_concept_preferred_term'] == 'Age':
            return datum['data_value']

def __build_dataframe( project_id, donor ):
	'''
	Build a dataframe with minimal information for this entity.
	'''

	id_namespace = 'tag:hubmapconsortium.org,2022:'
	headers = ['id_namespace', \
		'local_id', \
		'project_id_namespace', \
		'project_local_id', \
		'persistent_id', \
		'creation_time', \
		'granularity', \
		'sex', \
		'ethnicity', \
		'age_at_enrollment']

	df = pd.DataFrame(columns=headers)
	df = df.append({'id_namespace':id_namespace, \
			'local_id':donor['id'], \
			'project_id_namespace':id_namespace, \
			'project_local_id':project_id, \
			'persistent_id':donor['id'], \
			'granularity':__get_subject_granularity(donor), \
			'sex':__get_subject_sex(donor), \
			'ethnicity':__get_subject_ethnicity(donor), \
			'age_at_enrollment':__get_age_at_enrollment(donor)}, ignore_index=True)

	return df

def create_manifest( project_id, donor, output_directory ):
    filename = os.path.join( output_directory, 'subject.tsv' )
    df = __build_dataframe( project_id, donor )
    df.to_csv( filename, sep="\t", index=False)

    return True

import pandas as pd
import os

def __get_subject_granularity(donor):
	return 'cfde_subject_granularity:0'

def __get_subject_ethnicity(info):
	return ''

def __get_subject_age_at_enrollment( info ):
    for datum in info:
        if datum['grouping_concept_preferred_term'] == 'Age':
            return datum['data_value']

def __get_subject_race( info ):
    for datum in info:
        if datum['grouping_concept_preferred_term'] == 'Race':
            race = datum['preferred_term']
            break
        
    labels = {}
    labels['American Indian or Alaskan Native'] = 'cfde_subject_race:0'
    labels['Asian or Pacific Islander'] = 'cfde_subject_race:1'
    labels['Black'] = 'cfde_subject_race:2'
    labels['White'] = 'cfde_subject_race:3'
    labels['Other'] = 'cfde_subject_race:4'
    
    return labels[race]
        
def __get_subject_sex( info ):
    for datum in info:
        if datum['grouping_concept_preferred_term'] == 'Sex':
            sex = datum['preferred_term']
            break
    
    labels = {}
    labels['Indeterminate'] = 'cfde_subject_sex:0'
    labels['Female'] = 'cfde_subject_sex:1'
    labels['Male'] = 'cfde_subject_sex:2'
    labels['Intersex'] = 'cfde_subject_sex:3'
    labels['Transexual (MTF)'] = 'cfde_subject_sex:4'
    labels['Transexual (FTM)'] = 'cfde_subject_sex:5'
    
    return labels[sex]
        
def __get_subject_age_at_enrollment( info ):
    for datum in info:
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
	
	try:
		metadata = donor.metadata['organ_donor_data']
	except:
		metadata = donor.metadata['living_donor_data']

	df = df.append({'id_namespace':id_namespace, \
			'local_id':donor.hubmap_id, \
			'project_id_namespace':id_namespace, \
			'project_local_id':project_id, \
			'persistent_id':donor.hubmap_id, \
			'granularity':__get_subject_granularity(donor), \
			'sex':__get_subject_sex(metadata), \
			'age_at_enrollment':__get_subject_age_at_enrollment(metadata)}, ignore_index=True)

	return df

def create_manifest( project_id, donor, output_directory ):
    filename = os.path.join( output_directory, 'subject.tsv' )
    df = __build_dataframe( project_id, donor )
    df.to_csv( filename, sep="\t", index=False)

    return True

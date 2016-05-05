-- ----------------------------------------------------------------------------
Purpose: 	Voter ID Format:
File name: 	voterid_data_format.txt
Date effective: February 9, 2014
Last updated:   March 24, 2015
Job: [ETL - DMV Voter ID - FTP Export - Weekly]
Format: Tab Delimited

-- ----------------------------------------------------------------------------

county_cd		char  5		County Code
first_name		char 20		First name
last_name		char 25		Last name
middle_name		char 20		Middle name
name_suffix_lbl		char  4		Name suffix
res_addr1		char 25		Residential address line one
res_addr2		char 25		Residential address line two
res_state		char  2		State
res_city		char 25		City
res_zip			char  5		Zip
res_zip4		char  4		Zip 4
race_cd			char  1		Race code
ethnic_cd		char  2		Ethnic code
gender_cd		char  1		Gender code
age			char  3         Age
party_cd                char  3         Party code
dmv_timestamp		char 10		DMV date


-- ----------------------------------------------------------------------------
race_cd
-- ----------------------------------------------------------------------------
A  	ASIAN                                                       
B  	BLACK or AFRICAN AMERICAN                                   
I  	INDIAN AMERICAN or ALASKA NATIVE                            
M  	TWO or MORE RACES                                           
O  	OTHER                                                       
U  	UNDESIGNATED                                                
W  	WHITE                                                       

-- ----------------------------------------------------------------------------
ethnic_cd
-- ----------------------------------------------------------------------------
HL 	HISPANIC or LATINO
NL 	NOT HISPANIC or NOT LATINO
UN 	UNDESIGNATED

-- ----------------------------------------------------------------------------
gender_cd
-- ----------------------------------------------------------------------------
F   	FEMALE                                                      
M   	MALE                                                        
U   	UNDESIGNATED                                                
-- ----------------------------------------------------------------------------

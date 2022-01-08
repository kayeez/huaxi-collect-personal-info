from os.path import dirname, abspath
from pathlib import Path
from urllib.parse import unquote

import pandas as pd

current_dir = dirname(abspath(__file__))
source_dir = f'{current_dir}/source'
unzipped_dir = f'{current_dir}/unzipped'
merged_dir = f'{current_dir}/../merged'
column_mapping = {
    'name': {
        'df_column': '1、您的美名：',
        'file_dir_name': '您的美名',
        'not_file': True,
    },
    'id_card_front_side': {
        'df_column': '2、身份证-正面',
        'file_dir_name': '身份证-正面',
    },
    'id_card_back_side': {
        'df_column': '3、身份证-反面',
        'file_dir_name': '身份证-反面',
    },
    'inch_photo': {
        'df_column': '4、寸照',
        'file_dir_name': '寸照',
    },
    'academic_certificate_1': {
        'df_column': '5、学历证书-1',
        'file_dir_name': '学历证书-1',
    },
    'academic_certificate_2': {
        'df_column': '6、学历证书-2',
        'file_dir_name': '学历证书-2',
    },
    'academic_certificate_3': {
        'df_column': '7、学历证书-3',
        'file_dir_name': '学历证书-3',
    },
    'academic_certificate_4': {
        'df_column': '8、学历证书-4',
        'file_dir_name': '学历证书-4',
    },
    'nurse_title': {
        'df_column': '9、护士资格证',
        'file_dir_name': '护士职称',

    },
    'senior_nurse_title': {
        'df_column': '10、护师职称',
        'file_dir_name': '护师职称',
    },
    'nurse_manager_title': {
        'df_column': '11、主管护师职称',
        'file_dir_name': '主管护师职称',
    },
    'nurse_license_1st_page': {
        'df_column': '12、护士执照-第一页（本人照片）',
        'file_dir_name': '护士执照-第一页（本人照片）',
    },
    'nurse_license_2nd_page': {
        'df_column': '13、护士执照-第二页（注册编号）',
        'file_dir_name': '护士执照-第二页（注册编号）',
    },
    'nurse_license_3rd_page': {
        'df_column': '14、护士执照-第三页（延续注册）',
        'file_dir_name': '护士执照-第三页（延续注册）',
    },
    'nurse_license_4th_page': {
        'df_column': '15、护士执照-第四页（变更注册）',
        'file_dir_name': '护士执照-第四页（变更注册）',
    },
}


def copy_file(source_path, target_path):
    with open(source_path, 'rb') as source_file:
        with open(target_path, 'wb') as target_file:
            target_file.write(source_file.read())


if __name__ == '__main__':
    excel_name = '145736566_2_新生儿科10楼个人信息收集_16_13.xlsx'
    file_resource_dir = '145736566_附件'
    df = pd.read_excel(f'{source_dir}/{excel_name}')
    rename_columns = {}
    for key, item in column_mapping.items():
        rename_columns[item['df_column']] = key
    not_file_columns = [key for key, item in column_mapping.items() if item.get('not_file', False)]
    df = df.rename(columns=rename_columns)
    records = df.to_dict('records')
    for record in records:
        name = record['name']
        personal_dir = f'{merged_dir}/{name}'
        Path(personal_dir).mkdir(parents=True, exist_ok=True)
        not_uploaded = []
        for key, item in column_mapping.items():
            if key in not_file_columns:
                continue
            file_url: str = str(record[key])
            if file_url == '(空)' or file_url == 'nan':
                not_uploaded.append({item['df_column']})
            else:
                file_name = unquote(file_url)[unquote(file_url).index('?') + 1:]
                file_name = file_name[file_name.index('?attname=') + len('?attname='):file_name.index('&')]
                file_name = unquote(file_name).replace('+', ' ')
                person_index = file_name[:file_name.index('_')]
                file_name_with_index = file_name[file_name.index('_'):]
                source_file_name = f'序号{person_index}_{name}{file_name_with_index}'
                file_suffix = source_file_name[source_file_name.rindex('.'):]
                file_path = f"{source_dir}/{file_resource_dir}/{source_file_name}"
                target_file = f"{personal_dir}/{item['file_dir_name']}{file_suffix}"
                copy_file(file_path, target_file)
        if len(not_uploaded) > 0:
            print('---------------------------------------------------')
            print(f'{name} not uploaded {not_uploaded}')
            print('---------------------------------------------------')

import io
import os.path
import zipfile
from os.path import dirname, abspath
from pathlib import Path

import pandas as pd

current_dir = dirname(abspath(__file__))
source_dir = f'{current_dir}/source'
unzipped_dir = f'{current_dir}/unzipped'
merged_dir = f'{current_dir}/../merged'
csv_file_name = 'collected_total.csv'
column_mapping = {
    'name': {
        'df_column': '您的美名',
        'file_dir_name': '您的美名',
        'not_file': True,
    },
    'id_card_front_side': {
        'df_column': '身份证-正面',
        'file_dir_name': '身份证正面',
    },
    'id_card_back_side': {
        'df_column': '身份证-反面',
        'file_dir_name': '身份证反面',
    },
    'inch_photo': {
        'df_column': '寸照',
        'file_dir_name': '寸照',
    },
    'nurse_title': {
        'df_column': '护士职称',
        'file_dir_name': '护士职称',

    },
    'senior_nurse_title': {
        'df_column': '护师职称',
        'file_dir_name': '护师职称',
    },
    'nurse_manager_title': {
        'df_column': '主管护师职称',
        'file_dir_name': '主管护师职称',
    },
    'nurse_license_1st_page': {
        'df_column': '护士执照-第一页（本人照片）',
        'file_dir_name': '护士执照第一页本人照',
    },
    'nurse_license_2nd_page': {
        'df_column': '护士执照-第二页（注册编号）',
        'file_dir_name': '护士执照第二页注册编',
    },
    'nurse_license_3rd_page': {
        'df_column': '护士执照-第三页（延续注册）',
        'file_dir_name': '护士执照第三页延续注',
    },
    'nurse_license_4th_page': {
        'df_column': '护士执照-第四页（变更注册）',
        'file_dir_name': '护士执照第四页变更注',
    },
    'submitter': {
        'df_column': '提交人',
        'file_dir_name': '提交人',
        'not_file': True,
    },
    'modifier': {
        'df_column': '修改人',
        'file_dir_name': '修改人',
        'not_file': True,
    },

}


def extract(parent_dir, zip_file_name: str):
    absolute_path = os.path.join(parent_dir, zip_file_name)
    if not zip_file_name.endswith('.zip'):
        raise Exception(f'{absolute_path} is not an zip file')
    z = zipfile.ZipFile(absolute_path)
    for sub_file_full_name in z.namelist():
        if sub_file_full_name.endswith('.zip'):
            dir_name = f"{unzipped_dir}/{sub_file_full_name[:sub_file_full_name.index('_')]}"
            Path(dir_name).mkdir(parents=True, exist_ok=True)
            content = io.BytesIO(z.read(sub_file_full_name))
            zip_file = zipfile.ZipFile(content)
            for i in zip_file.namelist():
                zip_file.extract(i, dir_name)
        elif sub_file_full_name.endswith('.csv'):
            z.extract(sub_file_full_name, unzipped_dir)
            os.rename(f'{unzipped_dir}/{sub_file_full_name}', f'{unzipped_dir}/{csv_file_name}')


def copy_file(source_path, target_path):
    with open(source_path, 'rb') as source_file:
        with open(target_path, 'wb') as target_file:
            target_file.write(source_file.read())


if __name__ == '__main__':
    Path(unzipped_dir).mkdir(parents=True, exist_ok=True)
    Path(merged_dir).mkdir(parents=True, exist_ok=True)
    extract(source_dir, '新生儿科10楼个人资料收集数据和附件_20220104092054.zip')
    df = pd.read_csv(f'{unzipped_dir}/{csv_file_name}')
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
            file_name: str = str(record[key])
            if file_name == 'nan':
                not_uploaded.append({item['df_column']})
            else:
                file_suffix = file_name[file_name.rindex('.'):]
                file_path = f"{unzipped_dir}/{item['file_dir_name']}/{file_name}"
                target_file = f"{personal_dir}/{item['df_column']}{file_suffix}"
                copy_file(file_path, target_file)
        if len(not_uploaded) > 0:
            print('---------------------------------------------------')
            print(f'{name} not uploaded {not_uploaded}')
            print('---------------------------------------------------')
    print('merged all files')

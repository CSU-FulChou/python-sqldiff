import sys

import click
import mysql.connector


def get_connection(information):
    """
    获取数据库连接对象
    """
    connection = None
    try:
        index = information.rindex('@')
        user = information[0:index].split(':', 1)
        host = information[index + 1:].split(':', 1)
        db_config = {
            'user': user[0],
            'password': user[1],
            'host': host[0],
            'port': host[1],
            'database': 'information_schema',
            'charset': 'utf8',
            'autocommit': True,
            'raise_on_warnings': True
        }
        connection = mysql.connector.connect(**db_config)
    except Exception as e:
        click.secho('ERROR: %s' % e, fg='red', err=True)
        sys.exit(1)
    return connection


def get_schema(connection, database):
    """
    获取 对应数据库信息：
    数据库是否存在，默认字符集信息
    """
    cursor_schema = connection.cursor(dictionary=True)
    query_schema = "SELECT * FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = '%s'" % database
    cursor_schema.execute(query_schema)
    schema_data = cursor_schema.fetchone()
    if cursor_schema.rowcount <= 0:
        raise Exception('源数据库 `%s` 不存在。' % database)
    cursor_schema.close()
    return True, schema_data['DEFAULT_CHARACTER_SET_NAME']


def get_table(connection, database):
    """
    获取 对应数据库全部表信息：
    """
    cursor_table = connection.cursor(dictionary=True)
    query_table = "SELECT * FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' " \
                         "ORDER BY `TABLE_NAME` ASC" % database
    cursor_table.execute(query_table)
    table_data_list = cursor_table.fetchall()
    if cursor_table.rowcount <= 0:
        raise Exception('源数据库 `%s` 没有数据表。' % database)

    cursor_table.close()
    table_data_dic = {}
    for v in table_data_list:
        table_data_dic[v['TABLE_NAME']] = v
    return table_data_dic


def drop_table(source_table, target_table):
    """
    返回 target_table 应该 drop 的 语句
    """
    diff_sql = []
    diff = []
    for target_table_name, target_table_data in target_table.items():
        if target_table_name not in source_table:
            diff_sql.append("DROP TABLE IF EXISTS `%s`;" % target_table_name)
            diff.append('TABLE: {} SRC no exist but DES exist, need drop'.format(target_table_name))
    return diff_sql, diff


def get_column(connection, database, table_name):
    cursor_column = connection.cursor(dictionary=True)

    query_column = "SELECT * FROM `information_schema`.`COLUMNS` " \
                          "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' " \
                          "ORDER BY `ORDINAL_POSITION` ASC" % \
                          (database, table_name)

    cursor_column.execute(query_column)

    column_data_t = cursor_column.fetchall()
    column_data_count = cursor_column.rowcount
    cursor_column.close()
    return column_data_count, column_data_t


def get_column_dic_and_pos(column_data_list):
    columns = {}
    columns_pos = {}
    for source_column_data in column_data_list:
        column_data = filter_column(source_column_data)
        columns[column_data['COLUMN_NAME']] = column_data
        columns_pos[column_data['ORDINAL_POSITION']] = column_data
    return columns, columns_pos


def get_statistic_t(connection, database, table_name):
    cursor_statistic = connection.cursor(dictionary=True)

    query_statistic = "SELECT * FROM `information_schema`.`STATISTICS` " \
                             "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'" % \
                             (database, table_name)

    cursor_statistic.execute(query_statistic)
    statistic_data_t = cursor_statistic.fetchall()
    statistic_data_count = cursor_statistic.rowcount
    cursor_statistic.close()

    return statistic_data_count, statistic_data_t


def get_statistics(statistic_data_t):
    statistics = {}
    for statistic_data in statistic_data_t:
        if statistic_data['INDEX_NAME'] in statistics:
            statistics[statistic_data['INDEX_NAME']].update({
                statistic_data['SEQ_IN_INDEX']: filter_statistics(statistic_data)
            })
        else:
            statistics[statistic_data['INDEX_NAME']] = {
                statistic_data['SEQ_IN_INDEX']: filter_statistics(statistic_data)
            }
    return statistics



@click.command()
@click.option("--source", required=True, help="指定源服务器。(格式: <user>:<password>@<host>:<port>)")
@click.option("--target", help="指定目标服务器。(格式: <user>:<password>@<host>:<port>)")
@click.option("--db", required=True, help="指定数据库。(格式: <source_db>:<target_db>)")
@click.pass_context
def mysqldiff(ctx, source, target, db):
    """差异 数据库结构差异(target 2 source) 工具
    :param source :输入指定源服务器(格式: <user>:<password>@<host>:<port>)
    :param target :输入指定目标服务器(格式: <user>:<password>@<host>:<port>)
    :param db     :指定数据库(格式: <source_db>:<target_db>)
    输出 diff 以及 alter 具体语句
    """
    source_connection = None
    target_connection = None

    try:
        source_database, target_database = db.split(':', 1)

        source_connection = get_connection(source)
        source_exist, source_default_character = get_schema(source_connection, source_database)
        source_schema_data = {'DEFAULT_CHARACTER_SET_NAME': source_default_character}
        if source_exist:
            print('source database default table CHARACTER name is {}'.format(source_default_character))

        source_table_data_dic = get_table(source_connection, source_database)
        # target:
        if target is None:
            target_connection = source_connection
        else:
            target_connection = get_connection(target)
        target_exist, target_default_character = get_schema(target_connection, target_database)
        if target_exist:
            print('target database default table CHARACTER name is {}'.format(target_default_character))

        target_table_data_dic = get_table(target_connection, target_database)

        diff_sql = []
        diff = []
        # DROP TABLE...
        diff_sql_drop, diff_drop = drop_table(source_table_data_dic, target_table_data_dic)
        diff_sql += diff_sql_drop
        diff += diff_drop

        for source_table_name, source_table_data in source_table_data_dic.items():
            if source_table_name in target_table_data_dic:
                if source_table_data_dic[source_table_name]['ENGINE'] != source_table_data_dic[source_table_name]['ENGINE']:
                    diff.append('TABLE: {} SRC ENGINE is {} but DES ENGINE is {}'.format(source_table_name, source_table_data_dic[source_table_name]['ENGINE'], source_table_data_dic[source_table_name]['ENGINE']))
                if source_table_data_dic[source_table_name]['TABLE_COLLATION'] != source_table_data_dic[source_table_name]['TABLE_COLLATION']:
                    diff.append('TABLE: {} SRC TABLE_COLLATION is {} but DES TABLE_COLLATION is {}'.format(source_table_name, source_table_data_dic[source_table_name]['TABLE_COLLATION'], source_table_data_dic[source_table_name]['TABLE_COLLATION']))
                # ALTER TABLE
                source_column_data_count, source_column_data_t = get_column(source_connection, source_database, source_table_name)
                target_column_data_count, target_column_data_t = get_column(target_connection, target_database, source_table_name)
                # ALTER LIST...
                alter_tables = []
                alter_columns = []
                alter_keys = []
                # for column
                if source_column_data_count > 0 and target_column_data_count > 0:
                    source_columns, source_columns_pos = get_column_dic_and_pos(source_column_data_t)
                    target_columns, target_columns_pos = get_column_dic_and_pos(target_column_data_t)
                    if source_columns_pos != target_columns_pos:
                        alter_tables.append("ALTER TABLE `%s`" % source_table_name)
                        # drop column
                        for column_name, column in target_columns.items():
                            if column_name not in source_columns:
                                diff.append('TABLE: {} COLUMN: {} SRC no exist but DES exist, need drop DES column'.format(source_table_name, column_name))  # TAG 6:30
                                target_columns = reset_calc_position(column_name, column['ORDINAL_POSITION'],
                                                                     target_columns, 3)
                                alter_columns.append("  DROP COLUMN `%s`" % column_name)

                        # add column
                        for column_name, column in source_columns.items():
                            if column_name not in target_columns:
                                diff.append('TABLE: {} COLUMN: {} SRC exist but DES not exist, need add DES column'.format(source_table_name, column_name))
                                null_able = get_col_default_null_able_info(column)
                                character = extra = ''
                                if column['CHARACTER_SET_NAME'] is not None:
                                    if column['CHARACTER_SET_NAME'] != source_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                                        character = ' CHARACTER SET %s' % column['CHARACTER_SET_NAME']
                                if column['EXTRA'] != '':
                                    extra = ' %s' % column['EXTRA'].upper()

                                after = get_column_after(column['ORDINAL_POSITION'], source_columns_pos)

                                # 重新计算字段位置
                                target_columns = reset_calc_position(column_name, column['ORDINAL_POSITION'],
                                                                     target_columns, 1)

                                alter_columns.append(
                                    "  ADD COLUMN `{column_name}` {column_type}{character}{null_able}{extra} {after}".format(
                                        column_name=column_name, column_type=column['COLUMN_TYPE'], character=character,
                                        null_able=null_able, extra=extra, after=after))

                        # modify column
                        for column_name, column in source_columns.items():
                            if column_name in target_columns:
                                if column != target_columns[column_name]:
                                    diff.append('TABLE: {} COLUMN: {} SRC and DES is different, need modify DES column'.format(source_table_name, column_name))
                                    null_able = get_col_default_null_able_info(column)
                                    character = extra = ''
                                    if column['CHARACTER_SET_NAME'] is not None:
                                        if column['CHARACTER_SET_NAME'] != source_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                                            character = ' CHARACTER SET %s' % column['CHARACTER_SET_NAME']

                                    if column['EXTRA'] != '':
                                        extra = ' %s' % column['EXTRA'].upper()

                                    after = get_column_after(column['ORDINAL_POSITION'], source_columns_pos)

                                    # 重新计算字段位置
                                    target_columns = reset_calc_position(column_name, column['ORDINAL_POSITION'],
                                                                         target_columns, 2)
                                    alter_columns.append(
                                        "  MODIFY COLUMN `{column_name}` {column_type}{character}{null_able}{extra} {after}".format(
                                            column_name=column_name, column_type=column['COLUMN_TYPE'], character=character, null_able=null_able,
                                            extra=extra, after=after))

                # for index
                source_statistic_data_count, source_statistic_data_t = get_statistic_t(source_connection, source_database, source_table_name)
                _, target_statistic_data_t = get_statistic_t(target_connection, target_database, source_table_name)

                if source_statistic_data_count > 0:
                    source_statistics = get_statistics(source_statistic_data_t)
                    target_statistics = get_statistics(target_statistic_data_t)

                    if source_statistics != target_statistics:
                        if not alter_tables:
                            alter_tables.append("ALTER TABLE `%s`" % source_table_name)
                        # index diff
                        same_statistics = []
                        for target_index_name, target_statistic in target_statistics.items():
                            for source_index_name, source_statistic in source_statistics.items():
                                if target_statistic == source_statistic:
                                    same_statistics.append(target_statistic)

                        # drop
                        for index_name, statistic in target_statistics.items():
                            if statistic not in same_statistics:
                                diff.append('TABLE: {} INDEX: {} SRC not exist  but DES exist, need drop DES index'.format(source_table_name, index_name))
                        # add
                        for index_name, statistic in source_statistics.items():
                            if statistic not in same_statistics:
                                diff.append('TABLE: {} INDEX: {} SRC exist but DES not exist, need add DES index'.format(source_table_name, index_name))



                        # drop index for index name
                        for target_index_name, target_statistic in target_statistics.items():
                            if target_index_name not in source_statistics:
                                    if 'PRIMARY' == target_index_name:
                                        alter_keys.append("  DROP PRIMARY KEY")
                                    else:
                                        alter_keys.append("  DROP INDEX `%s`" % target_index_name)
                        # modify index
                        for index_name, statistic in source_statistics.items():
                            if index_name in target_statistics:
                                # modify index = DROP INDEX ... AND ADD KEY ...
                                if statistic != target_statistics[index_name]:
                                    if 'PRIMARY' == index_name:
                                        alter_keys.append("  DROP PRIMARY KEY")
                                    else:
                                        alter_keys.append("  DROP INDEX `%s`" % index_name)

                                    alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic))
                            else:
                                # ADD KEY
                                alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic))

                        if alter_keys:
                            for alter_key in alter_keys:
                                alter_columns.append(alter_key)

                if alter_columns:
                    for alter_column in alter_columns:
                        if alter_column == alter_columns[-1]:
                            column_dot = ';'
                        else:
                            column_dot = ','
                        alter_tables.append('%s%s' % (alter_column, column_dot))

                if alter_tables:
                    diff_sql.append('\n'.join(alter_tables))

            else:
                # CREATE TABLE...
                source_column_data_count, source_column_data = get_column(source_connection, source_database, source_table_name)

                if source_column_data_count > 0:
                    source_statistics_data_count, source_statistics_data_t = get_statistic_t(source_connection, source_database, source_table_name)

                    create_tables = ["CREATE TABLE IF NOT EXISTS `%s` (" % source_table_name]

                    diff.append('TABLE: {} SRC exist but DES not exist, need add DES table'.format(source_table_name))
                    # COLUMN...
                    for column in source_column_data:
                        null_able = get_col_default_null_able_info(column)

                        character = extra = dot = ''

                        if column['CHARACTER_SET_NAME'] is not None:
                            if column['CHARACTER_SET_NAME'] != source_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                                character = ' CHARACTER SET %s' % column['CHARACTER_SET_NAME']

                        if column['EXTRA'] != '':
                            extra = ' %s' % column['EXTRA'].upper()

                        if column != source_column_data[-1] or source_statistics_data_count > 0:
                            dot = ','

                        create_tables.append(
                            "  `{column_name}` {column_type}{character}{null_able}{extra}{dot}".format(
                                column_name=column['COLUMN_NAME'], column_type=column['COLUMN_TYPE'],
                                character=character,
                                null_able=null_able, extra=extra, dot=dot))

                    # key
                    create_tables_keys = []
                    if source_statistics_data_count > 0:
                        source_statistics_data_dic = get_statistics(source_statistics_data_t)

                        for index_name, source_statistics_data in source_statistics_data_dic.items():
                            create_tables_keys.append("  {key_slot}".format(key_slot=get_add_keys(index_name, source_statistics_data)))

                    create_tables.append(",\n".join(create_tables_keys))
                    create_tables.append(
                        ") ENGINE={engine} DEFAULT CHARSET={charset};".format(engine=source_table_data['ENGINE'],
                                                                              charset=source_schema_data['DEFAULT_CHARACTER_SET_NAME']))
                    diff_sql.append("\n".join(create_tables))

        if diff_sql:
            print('\n'.join(diff))
            print('SET NAMES %s;\n' % source_schema_data['DEFAULT_CHARACTER_SET_NAME'])
            print("\n\n".join(diff_sql))

    except Exception as e:
        click.secho('ERROR: %s' % e, fg='red', err=True)
        sys.exit(1)
    finally:
        if source_connection is not None:
            source_connection.close()
        if target_connection is not None:
            target_connection.close()


def filter_column(column):
    return {
        'COLUMN_NAME': column['COLUMN_NAME'],
        'ORDINAL_POSITION': column['ORDINAL_POSITION'],
        'COLUMN_DEFAULT': column['COLUMN_DEFAULT'],
        'IS_NULLABLE': column['IS_NULLABLE'],
        'DATA_TYPE': column['DATA_TYPE'],
        'CHARACTER_MAXIMUM_LENGTH': column['CHARACTER_MAXIMUM_LENGTH'],
        'CHARACTER_OCTET_LENGTH': column['CHARACTER_OCTET_LENGTH'],
        'NUMERIC_PRECISION': column['NUMERIC_PRECISION'],
        'NUMERIC_SCALE': column['NUMERIC_SCALE'],
        'DATETIME_PRECISION': column['DATETIME_PRECISION'],
        'CHARACTER_SET_NAME': column['CHARACTER_SET_NAME'],
        'COLLATION_NAME': column['COLLATION_NAME'],
        'COLUMN_TYPE': column['COLUMN_TYPE'],
        'EXTRA': column['EXTRA']
    }


def get_col_default_null_able_info(column):
    if column['IS_NULLABLE'] == 'NO':
        if column['COLUMN_DEFAULT'] is not None:
            if column['DATA_TYPE'] == 'timestamp':
                null_able = " NOT NULL DEFAULT %s" % column['COLUMN_DEFAULT']
            else:
                null_able = " NOT NULL DEFAULT '%s'" % column['COLUMN_DEFAULT']
        else:
            null_able = " NOT NULL"
    else:
        if column['COLUMN_DEFAULT'] is not None:
            if column['DATA_TYPE'] == 'timestamp':
                null_able = " NULL DEFAULT %s" % column['COLUMN_DEFAULT']
            else:
                null_able = " DEFAULT '%s'" % column['COLUMN_DEFAULT']
        else:
            null_able = ' DEFAULT NULL'

    return null_able


def get_column_after(ordinal_position, column_pos):
    pos = ordinal_position - 1
    if pos in column_pos:
        return "AFTER `%s`" % column_pos[pos]['COLUMN_NAME']
    else:
        return "FIRST"


def get_add_keys(index_name, statistic):
    non_unique = statistic[1]['NON_UNIQUE']

    if 1 == non_unique:
        columns_name = []

        for k in sorted(statistic):
            sub_part = ''

            if statistic[k]['SUB_PART'] is not None:
                sub_part = '(%d)' % statistic[k]['SUB_PART']

            columns_name.append(
                "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

        return "KEY `{index_name}` ({columns_name})".format(index_name=index_name, columns_name=",".join(columns_name))
    else:
        columns_name = []

        if 'PRIMARY' == index_name:

            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}{sub_part}`".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "PRIMARY KEY ({columns_name})".format(columns_name=",".join(columns_name))
        else:
            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "UNIQUE KEY `{index_name}` ({columns_name})".format(index_name=index_name,
                                                                       columns_name=",".join(columns_name))


def reset_calc_position(column_name, local_pos, target_columns, status):
    if 1 == status:
        # ADD ...
        for k, v in target_columns.items():
            cur_pos = v['ORDINAL_POSITION']
            if cur_pos >= local_pos:
                target_columns[k]['ORDINAL_POSITION'] = target_columns[k]['ORDINAL_POSITION'] + 1

    elif 2 == status:
        # MODIFY ...
        if column_name in target_columns:
            target_columns[column_name]['ORDINAL_POSITION'] = local_pos
    elif 3 == status:
        # DROP ...
        for k, v in target_columns.items():
            cur_pos = v['ORDINAL_POSITION']

            if cur_pos >= local_pos:
                target_columns[k]['ORDINAL_POSITION'] = target_columns[k]['ORDINAL_POSITION'] - 1

    return target_columns


def filter_statistics(statistic):
    return {
        'NON_UNIQUE': statistic['NON_UNIQUE'],
        'INDEX_NAME': statistic['INDEX_NAME'],
        'SEQ_IN_INDEX': statistic['SEQ_IN_INDEX'],
        'COLUMN_NAME': statistic['COLUMN_NAME'],
        'SUB_PART': statistic['SUB_PART'],
        'INDEX_TYPE': statistic['INDEX_TYPE'],
    }

if __name__ == '__main__':
    mysqldiff(obj={})

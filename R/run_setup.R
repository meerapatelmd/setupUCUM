#' @title
#' Load UCUM into Postgres
#' @description
#' The Excel file downloaded from
#' \url{https://ucum.org/trac/wiki/adoption/common} is loaded into Postgres.The
#' file release dates are taken along with the table names and row
#' counts are logged to a separate table after the loading is completed.
#' @param conn Connection to a Postgres database.
#' @param schema Target schema. Default: 'ucum'.
#' @param log_schema Schema for the table that logs the process, Default: 'public'
#' @param log_table_name Name of log table, Default: 'setup_ucum_log'.
#' @rdname run_setup
#' @export
#' @importFrom pg13 schema_exists drop_cascade send ls_tables query render_row_count table_exists read_table drop_table write_table
#' @importFrom SqlRender render
#' @importFrom purrr map set_names
#' @importFrom dplyr bind_rows rename mutate select everything
#' @importFrom tidyr pivot_wider
#' @importFrom cli cat_line cat_boxx
#' @importFrom tibble as_tibble




run_setup <-
  function(conn,
           path_to_excel,
           schema = "ucum",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           log_schema = "public",
           log_table_name = "setup_ucum_log") {


    df_list <-
    path_to_excel %>%
      readxl::excel_sheets() %>%
      purrr::set_names() %>%
      purrr::map(readxl::read_excel,
                 path = path_to_excel)

    target_sheet <-
    names(df_list)[!(names(df_list) %in% c("PREFACE", "REVISIONS LOG"))]

    log_version <-
    target_sheet %>%
      str_replace(pattern = "^.*?(v.*$)",
                  replacement = "\\1")

    df <-
      df_list[[target_sheet]]

    df <-
      df[-1,]

    df <-
      df %>%
      dplyr::select_at(vars(1:3)) %>%
      rubix::rename_fields(new_colnames = c("rowid", "code", "name"))


    if (pg13::schema_exists(conn = conn,
                        schema = schema)) {
      pg13::drop_cascade(conn = conn,
                         schema = schema,
                         verbose = verbose,
                         render_sql = render_sql,
                         render_only = render_only)
    }

    pg13::create_schema(conn = conn,
                        schema = schema)

    pg13::write_table(
      conn = conn,
      schema = schema,
      table_name = "ucum",
      data = df
    )



    #Log
        table_names <-
          pg13::ls_tables(conn = conn,
                          schema = schema,
                          verbose = verbose,
                          render_sql = render_sql)

        current_row_count <-
          table_names %>%
          purrr::map(function(x) pg13::query(conn = conn,
                                             sql_statement = pg13::render_row_count(schema = schema,
                                                                                  tableName = x))) %>%
          purrr::set_names(tolower(table_names)) %>%
          dplyr::bind_rows(.id = "Table") %>%
          dplyr::rename(Rows = count) %>%
          tidyr::pivot_wider(names_from = "Table",
                             values_from = "Rows") %>%
          dplyr::mutate(su_datetime = Sys.time(),
                        su_version = log_version,
                        su_schema = schema) %>%
          dplyr::select(su_datetime,
                        su_version,
                        su_schema,
                        dplyr::everything())



        if (pg13::table_exists(conn = conn,
                                schema = log_schema,
                                table_name = log_table_name)) {

                updated_log <-
                  dplyr::bind_rows(
                        pg13::read_table(conn = conn,
                                         schema = log_schema,
                                         table = log_table_name,
                                         verbose = verbose,
                                         render_sql = render_sql,
                                         render_only = render_only),
                        current_row_count)  %>%
                  dplyr::select(su_datetime,
                                su_version,
                                su_schema,
                                dplyr::everything())

        } else {
          updated_log <- current_row_count
        }

        pg13::drop_table(conn = conn,
                         schema = log_schema,
                         table = log_table_name,
                         verbose = verbose,
                         render_sql = render_sql,
                         render_only = render_only)

        pg13::write_table(conn = conn,
                          schema = log_schema,
                          table_name = log_table_name,
                          data = updated_log,
                          verbose = verbose,
                          render_sql = render_sql,
                          render_only = render_only)

        cli::cat_line()
        cli::cat_boxx("Log Results",
                      float = "center")
        print(tibble::as_tibble(updated_log))
        cli::cat_line()



      }

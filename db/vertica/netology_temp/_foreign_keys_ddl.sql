ALTER TABLE netology_temp.tableau_views ADD CONSTRAINT tableau_views_tableau_users_id_fk FOREIGN KEY (owner_id) REFERENCES netology_temp.tableau_users;
ALTER TABLE netology_temp.tableau_views ADD CONSTRAINT tableau_views_tableau_projects_id_fk FOREIGN KEY (project_id) REFERENCES netology_temp.tableau_projects;
ALTER TABLE netology_temp.tableau_views ADD CONSTRAINT tableau_views_tableau_workbooks_id_fk FOREIGN KEY (workbook_id) REFERENCES netology_temp.tableau_workbooks;

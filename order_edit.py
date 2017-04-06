#!/usr/bin/env python
from flask import Flask, render_template, redirect, jsonify
from flask_bootstrap import Bootstrap

from flask_wtf import FlaskForm
from wtforms import StringField, SelectField
from wtforms.validators import DataRequired
from flask_wtf.csrf import CSRFProtect

import metalchemy as a

app = Flask(__name__)
Bootstrap(app)
csrf = CSRFProtect(app)
csrf.init_app(app)

app.debug = True
app.secret_key = 's3cr3t'

cdbs = a.Cdb.all()
indexed_cdbs = {d.id:d for d in cdbs}


class MyForm(FlaskForm):
    CDB_CHOICES = [(None, "Select a database:")] + [(cdb.id, "{:4.4} - {}".format(cdb.id, cdb.name)) for cdb in cdbs]
    cdb = SelectField('Database', choices=CDB_CHOICES)
    lst = SelectField('List', choices=[])

    #name = StringField('name', validators=[DataRequired()])
    #lname = StringField('lname', validators=[DataRequired()])
    
    def init_cdb_combo(self):
        #cdbs = a.Cdb.all()
        self.cdb.choices = [(None, "Select a database:")]
        for cdb in cdbs:
            self.cdb.choices.append((cdb.id, "{:4.4} - {}".format(cdb.id, cdb.name)))
        


@app.route("/", methods=('GET',))
@csrf.exempt
def render_dblist():
    print("RENDER_DBLIST")
    form = MyForm()
    
    if form.validate_on_submit():
        # return redirect('/success')
        return render_template('success.html')
    return render_template('portal.html', form=form)


@app.route("/lists/<string:cdb_id>/", methods=('GET',))
@csrf.exempt
def get_lists(cdb_id):
    print("GET_LISTS", cdb_id)
    cdb = indexed_cdbs[cdb_id]
    options = [(None, "Select a databaselist:")] + [(l.id, "{:4.4} - {}".format(l.id, l.name)) for l in cdb.lists]
    return jsonify(options)


@app.route("/", methods=('POST',))
@csrf.exempt
def accept_form():
    print("ACCEPT FORM")
    
    #if form.validate_on_submit():
    #    return render_template('success.html')
    return render_template('success.html')


@app.route('/submit', methods=('GET', 'POST'))
def submit():
    #form = MyForm()
    #if form.validate_on_submit():
    #    return redirect('/success')
    #return render_template('success.html')
    print("SUBMIT!!!!!!!!!!!!!!!!!!!!!!!!!!")

if __name__ == "__main__":
    app.run()
    #app.run(host='0.0.0.0')

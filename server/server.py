from api import *

PORT = 8888

app = Flask(__name__, static_path='/static')
app.config['SERVER_NAME'] = "localhost:8888"

api = Api(app)

TEST_DATA_FOLDER = os.path.join(os.curdir, config['TEST_DATA_FOLDER'])
app.config['TEST_DATA_FOLDER'] = TEST_DATA_FOLDER

PUBLIC_DATA_FOLDER = os.path.join(os.curdir, config['PUBLIC_DATA_FOLDER'])
app.config['PUBLIC_DATA_FOLDER'] = PUBLIC_DATA_FOLDER

UPLOAD_FOLDER = os.path.join(os.curdir, config['UPLOAD_FOLDER'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

api.add_resource(Public_Data, '/api/public_data')
api.add_resource(Render_SVG, '/api/render_svg')
api.add_resource(UploadFile, '/api/upload')
api.add_resource(Data, '/api/data')
api.add_resource(GetProjectID, '/api/getProjectID')
api.add_resource(Project, '/api/project')
api.add_resource(Property, '/api/property')
api.add_resource(Specification, '/api/specification')
api.add_resource(Choose_Spec, '/api/choose_spec')
api.add_resource(Reject_Spec, '/api/reject_spec')
api.add_resource(Visualization_Data, '/api/visualization_data')
api.add_resource(Conditional_Data, '/api/conditional_data')
api.add_resource(Exported_Visualization_Spec, '/api/exported_spec')

if __name__ == '__main__':
    app.debug = True
    app.run(port=PORT)
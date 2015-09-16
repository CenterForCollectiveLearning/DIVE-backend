from flask import send_file
from flask.ext.restful import Resource, reqparse

import cairosvg
import cairocffi as cairo
from StringIO import StringIO

#####################################################################
# Endpoint returning exported image
#####################################################################
renderSVGPostParser = reqparse.RequestParser()
renderSVGPostParser.add_argument('data')
class Render(Resource):
    def post(self):
        args = renderSVGPostParser.parse_args()
        data = json.loads(args.get('data'))
        format = data['format']
        svg = data['svg']

        filename = 'visualization.%s' % format
        fout = open(filename, 'wb')

        mimetypes = {
            'svg': 'image/svg',
            'pdf': 'application/pdf',
            'png': 'image/png'
        }

        img_io = StringIO()
        bytestring = bytes(svg)
        if format == "png":
            cairosvg.svg2png(bytestring=bytestring, write_to=fout)
            cairosvg.svg2png(bytestring=bytestring, write_to=img_io)
        elif format == "pdf":
            cairosvg.svg2pdf(bytestring=bytestring, write_to=fout)
            cairosvg.svg2pdf(bytestring=bytestring, write_to=img_io)
        elif format == "svg":
            cairosvg.svg2svg(bytestring=bytestring, write_to=fout)
            cairosvg.svg2svg(bytestring=bytestring, write_to=img_io)
        else:
            cairosvg.svg2png(bytestring=bytestring, write_to=fout)
            cairosvg.svg2png(bytestring=bytestring, write_to=img_io)
        fout.close()

        img_io.seek(0)
        return send_file(img_io)  #, mimetype=mimetypes[format], as_attachment=True, attachment_filename=filename)

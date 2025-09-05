import sys
from .service import Service

def main():
    #  for c in Service('ec2').how_to_get('InstanceId'):
    #  for c in Service('ssm').how_to_get('Name', method='get_parameter_history'):
    for c in Service('ssm').how_to_get('instance'):
    #  shape = Service('lambda').make_method('create_function').model.input_shape.members['Runtime']
    #  for c in Service('lambda').how_to_get('Runtime', method='create_function', shape=shape):
        print(f'''DEBUG(tucson)\t{c = }''', file=sys.__stderr__)
        #  break
    #  print(Service('ec2').how_to_get('VpcId'))
    #  print(Service('elbv2').how_to_get('Attributes'))
    #  print(Service('ec2').how_to_get('associated enclave certificate iam roles CertificateArn'))
    #  for c in Service('dynamodb').how_to_get('Table'):
        #  print(f'''DEBUG(glut)  \t{c.score(KeySpec.from_str('Table')), c = }''', file=sys.__stderr__)

if __name__ == '__main__':
    main()

from diagrams import Diagram, Cluster, Edge
from diagrams.oci.database import DatabaseService
from diagrams.oci.storage import ObjectStorage, Buckets
from diagrams.oci.governance import Audit
from diagrams.programming.flowchart import Action
from diagrams.generic.virtualization import Virtualbox
from diagrams.oci.monitoring import Telemetry
from diagrams.onprem.queue import Kafka
from diagrams.custom import Custom
from diagrams.gcp.operations import Monitoring


with Diagram(name="Livesteram Data Reduction System Candidate 1", show=False):
    
    with Cluster("Instrument VM"):
        shared_place = Buckets("shared")
        ps_control = Monitoring("Process Controller")
        config = Audit("Congfiguration")

        with Cluster("Data Handling"):
            wf_control = DatabaseService("Workflow Control")
            with Cluster("Data Reduction Workflow"):
                data_reductions = [Custom("Sub Workflow 3", './docs/logo.png'),
                                Custom("Sub Workflow 2", './docs/logo.png'),
                               Custom("Sub Workflow 1", './docs/logo.png')]

        with Cluster("Visualization"):
            web_browser = ObjectStorage('Web Browser')
            dash_builder = Telemetry("Dashboard Builder")
            ps_control << web_browser

        kaf_listener = Kafka("Listener")
        kaf_listener >> wf_control
        wf_control >> data_reductions 
        data_reductions >> shared_place
        shared_place >> dash_builder >> shared_place
        web_browser << dash_builder
        

    kaf_producer = Kafka("Producer")
    kaf_producer << Edge(label='subscribe') << kaf_listener


with Diagram(name="Livesteram Data Reduction System Candidate 2", show=False):
    
    with Cluster("Instrument VM"):
        ps_control = Monitoring("Process Controller \n(backend)")
        config = Audit("Congfiguration")

        with Cluster("Visualization"):
            web_browser = ObjectStorage('Web Browser')
            dash_builder = Telemetry("Dashboard Builder")

            with Cluster("Data Handling"):
                with Cluster("Data Reduction Workflow"):
                    data_reductions = [Custom("Sub Workflow 3", './docs/logo.png'),
                                    Custom("Sub Workflow 2", './docs/logo.png'),
                                Custom("Sub Workflow 1", './docs/logo.png')]

            dash_builder >> Edge() << data_reductions
            dash_builder >> web_browser

        kaf_listener = Kafka("Listener")
        kaf_listener >> dash_builder

    kaf_producer = Kafka("Producer")
    kaf_producer << Edge(label='subscribe') << kaf_listener
    web_browser >> ps_control >> web_browser
    dash_builder << ps_control

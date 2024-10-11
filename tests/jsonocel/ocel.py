import pm4py
from pm4py.objects.ocel.obj import OCEL

o_type_schema: dict = {
    "name": "Invoice",
    "attributes": [{"name": "is_blocked", "type": "string"}]
}

e_type_schema: dict = {
    "name": "Approve Purchase Requisition",
    "attributes": [{"name": "pr_approver", "type": "string"}]
}

o_schema: dict = {
    "id": "R1",
    "type": "Invoice",
    "attributes": [{"name": "is_blocked", "time": "1970-01-01T00:00:00Z", "value": "No"}],
    "relationships": [{"objectId": "P1", "qualifier": "Payment from invoice"}]
}

e_schema: dict = {
    "id": "e1",
    "type": "Create Purchase Requisition",
    "time": "2022-01-09T14:00:00+00:00",
    "attributes": [{"name": "pr_creator", "value": "Mike"}],
    "relationships": [{"objectId": "PR1", "qualifier": "Regular placement of PR"}]
}
pm4py.read_ocel2()

ocel = OCEL(
    events=[
        {"name": "Create Purchase Requisition", "time": "2022-01-09T14:00:00+00:00",
         "attributes": [{"name": "pr_creator", "value": "Mike"}],
         "relationships": [{"objectId": "PR1", "qualifier": "Regular placement of PR"}]},
        {"name": "Approve Purchase Requisition", "time": "2022-01-09T14:00:00+00:00",
         "attributes": [{"name": "pr_approver", "value": "John"}],
         "relationships": [{"objectId": "PR1", "qualifier": "Regular placement of PR"}]},
        {"name": "Create Purchase Order", "time": "2022-01-09T14:00:00+00:00",
         "attributes": [{"name": "po_creator", "value": "John"}],
         "relationships": [{"objectId": "PO1", "qualifier": "Regular placement of PO"}]},
        {"name": "Approve Purchase Order", "time": "2022-01-09T14:00:00+00:00",
         "attributes": [{"name": "po_approver", "value": "Mike"}],
         "relationships": [{"objectId": "PO1", "qualifier": "Regular placement of PO"}]},
        {"name": "Create Invoice", "time": "2022-01-09T14:00:00+00:00",
         "attributes": [{"name": "invoice_creator", "value": "Mike"}],
         "relationships": [{"objectId": "I1", "qualifier": "Regular placement of Invoice"}]},
    ],
    objects=[
        {"id": "PR1", "type": "Purchase Requisition",
         "attributes": [{"name": "pr_creator", "time": "2022-01-09T14:00:00+00:00", "value": "Mike"}],
         "relationships": [{"objectId": "PO1", "qualifier": "Regular placement of PR"}]},
        {"id": "PO1", "type": "Purchase Order",
         "attributes": [{"name": "po_creator", "time": "2022-01-09T14:00:00+00:00", "value": "John"}],
         "relationships": [{"objectId": "I1", "qualifier": "Regular placement of PO"}]},
        {"id": "I1", "type": "Invoice",
         "attributes": [{"name": "invoice_creator", "time": "2022-01-09T14:00:00+00:00", "value": "Mike"}],
         "relationships": []},
    ]
)

print(ocel)
